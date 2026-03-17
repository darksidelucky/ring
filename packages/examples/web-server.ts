import 'dotenv/config'
import express from 'express'
import { fileURLToPath } from 'url'
import { dirname, join } from 'path'
import { RingApi } from 'ring-client-api'
import type { Response } from 'express'

const __dirname = dirname(fileURLToPath(import.meta.url))
const PORT = 3500

const refreshToken = process.env.RING_REFRESH_TOKEN
if (!refreshToken) {
  console.error('RING_REFRESH_TOKEN is not set in .env')
  process.exit(1)
}

const ringApi = new RingApi({ refreshToken, debug: false })

// ─── Brivo ───────────────────────────────────────────────────────────────────

const BRIVO_BASE = 'https://api.brivo.com/v1/api'
const BRIVO_AUTH = 'https://auth.brivo.com/oauth/token'

let brivoToken: string | null = null
let brivoTokenExpiry = 0

async function getBrivoToken(): Promise<string> {
  if (brivoToken && Date.now() < brivoTokenExpiry) return brivoToken

  const { BRIVO_API_KEY, BRIVO_USERNAME, BRIVO_PASSWORD } = process.env
  if (!BRIVO_API_KEY || !BRIVO_USERNAME || !BRIVO_PASSWORD) {
    throw new Error('Brivo credentials not set in .env')
  }

  const body = new URLSearchParams({
    grant_type: 'password',
    username: BRIVO_USERNAME,
    password: BRIVO_PASSWORD,
  })

  const basicCreds = Buffer.from(`${BRIVO_API_KEY}:${BRIVO_API_KEY}`).toString('base64')

  const res = await fetch(BRIVO_AUTH, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
      'Authorization': `Basic ${basicCreds}`,
      'api-key': BRIVO_API_KEY,
    },
    body: body.toString(),
  })

  if (!res.ok) {
    const text = await res.text()
    throw new Error(`Brivo auth failed ${res.status}: ${text}`)
  }

  const data: any = await res.json()
  brivoToken = data.access_token
  brivoTokenExpiry = Date.now() + (data.expires_in - 60) * 1000
  return brivoToken!
}

async function brivoGet(path: string): Promise<any> {
  const token = await getBrivoToken()
  const res = await fetch(`${BRIVO_BASE}${path}`, {
    headers: {
      'Authorization': `Bearer ${token}`,
      'api-key': process.env.BRIVO_API_KEY!,
    },
  })
  if (!res.ok) {
    const text = await res.text()
    throw new Error(`Brivo ${path} failed ${res.status}: ${text}`)
  }
  return res.json()
}

// ─────────────────────────────────────────────────────────────────────────────

// SSE clients waiting for motion events
const sseClients = new Set<Response>()

function broadcastMotionSnapshot(cameraId: number, cameraName: string, jpegBase64: string) {
  const payload = JSON.stringify({ cameraId, cameraName, jpegBase64, at: new Date().toISOString() })
  for (const client of sseClients) {
    client.write(`data: ${payload}\n\n`)
  }
}

// Subscribe to motion on all cameras at startup
async function subscribeMotion() {
  const cameras = await ringApi.getCameras()
  console.log(`Subscribing to motion on ${cameras.length} camera(s)…`)

  for (const camera of cameras) {
    camera.onMotionDetected.subscribe(async (motion) => {
      if (!motion) return
      console.log(`Motion detected on ${camera.name} — fetching snapshot…`)
      try {
        const snapshot = await camera.getSnapshot()
        const b64 = snapshot.toString('base64')
        broadcastMotionSnapshot(camera.id, camera.name, b64)
        console.log(`Snapshot sent for ${camera.name}`)
      } catch (e: any) {
        console.error(`Snapshot failed for ${camera.name}: ${e.message}`)
      }
    })
  }
}

subscribeMotion().catch((e) => console.error('Motion subscription failed:', e.message))

const app = express()
app.use(express.static(join(__dirname, 'public')))

// Health check
app.get('/api/status', (_req, res) => {
  res.json({ ok: true })
})

// SSE stream — browser connects here to receive motion snapshots in real time
app.get('/api/motion-stream', (req, res) => {
  res.set({
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'X-Accel-Buffering': 'no',
  })
  res.flushHeaders()
  res.write(': connected\n\n')

  sseClients.add(res)

  req.on('close', () => {
    sseClients.delete(res)
  })
})

// Locations + summary counts
app.get('/api/locations', async (_req, res) => {
  try {
    const locations = await ringApi.getLocations()
    const data = await Promise.all(
      locations.map(async (loc) => {
        const cameras = loc.cameras
        const devices = await loc.getDevices().catch(() => [])
        return {
          id: loc.id,
          name: loc.name,
          cameraCount: cameras.length,
          deviceCount: devices.length,
        }
      }),
    )
    res.json(data)
  } catch (e: any) {
    res.status(500).json({ error: e.message })
  }
})

// All cameras
app.get('/api/cameras', async (_req, res) => {
  try {
    const cameras = await ringApi.getCameras()
    res.json(
      cameras.map((c) => ({
        id: c.id,
        name: c.name,
        type: c.deviceType,
        batteryLevel: c.batteryLevel,
        isOnline: c.data.led_status !== 'off',
        hasLight: c.hasLight,
        hasSiren: c.hasSiren,
        location: c.data.description?.location_id,
      })),
    )
  } catch (e: any) {
    res.status(500).json({ error: e.message })
  }
})

// Recent events for a camera
app.get('/api/cameras/:id/events', async (req, res) => {
  try {
    const cameras = await ringApi.getCameras()
    const camera = cameras.find((c) => String(c.id) === req.params.id)
    if (!camera) {
      res.status(404).json({ error: 'Camera not found' })
      return
    }
    const { events } = await camera.getEvents({ limit: 20 })
    res.json(
      events.map((e) => ({
        id: e.ding_id_str,
        kind: e.kind,
        createdAt: e.created_at,
        recordingStatus: e.recording_status,
        answered: e.answered,
      })),
    )
  } catch (e: any) {
    res.status(500).json({ error: e.message })
  }
})

// Snapshot for a camera — returns JPEG image
app.get('/api/cameras/:id/snapshot', async (req, res) => {
  try {
    const cameras = await ringApi.getCameras()
    const camera = cameras.find((c) => String(c.id) === req.params.id)
    if (!camera) {
      res.status(404).json({ error: 'Camera not found' })
      return
    }
    const snapshot = await camera.getSnapshot()
    res.set('Content-Type', 'image/jpeg')
    res.set('Cache-Control', 'no-store')
    res.send(snapshot)
  } catch (e: any) {
    res.status(500).json({ error: e.message })
  }
})

// Devices at a location
app.get('/api/locations/:id/devices', async (req, res) => {
  try {
    const locations = await ringApi.getLocations()
    const location = locations.find((l) => String(l.id) === req.params.id)
    if (!location) {
      res.status(404).json({ error: 'Location not found' })
      return
    }
    const devices = await location.getDevices()
    res.json(
      devices.map((d) => ({
        zid: d.zid,
        name: d.name,
        type: d.deviceType,
        batteryLevel: (d.data as any).batteryLevel ?? null,
        tamperStatus: (d.data as any).tamperStatus ?? null,
      })),
    )
  } catch (e: any) {
    res.status(500).json({ error: e.message })
  }
})

// Brivo — recent access events
app.get('/api/brivo/events', async (req, res) => {
  try {
    const limit = Math.min(Number(req.query.limit) || 50, 100)
    const adminId = process.env.BRIVO_ADMIN_ID
    const path = adminId
      ? `/administrators/${encodeURIComponent(adminId)}/events?pageSize=${limit}&pageNo=0`
      : `/events?pageSize=${limit}&pageNo=0`
    const data = await brivoGet(path)
    res.json(data)
  } catch (e: any) {
    res.status(500).json({ error: e.message })
  }
})

// Brivo — list sites
app.get('/api/brivo/sites', async (_req, res) => {
  try {
    const data = await brivoGet('/sites?pageSize=100&pageNo=0')
    res.json(data)
  } catch (e: any) {
    res.status(500).json({ error: e.message })
  }
})

// Brivo — list doors for a site
app.get('/api/brivo/sites/:siteId/doors', async (req, res) => {
  try {
    const data = await brivoGet(`/sites/${encodeURIComponent(req.params.siteId)}/doors?pageSize=100&pageNo=0`)
    res.json(data)
  } catch (e: any) {
    res.status(500).json({ error: e.message })
  }
})

app.listen(PORT, () => {
  console.log(`Ring dashboard running at http://localhost:${PORT}`)
})
