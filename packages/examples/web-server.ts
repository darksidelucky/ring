import 'dotenv/config'
import express from 'express'
import cors from 'cors'
import helmet from 'helmet'
import rateLimit from 'express-rate-limit'
import { fileURLToPath } from 'url'
import { dirname, join } from 'path'
import { RingApi } from 'ring-client-api'
import type { Response } from 'express'

const __dirname = dirname(fileURLToPath(import.meta.url))
const PORT = 3500

// ── Startup validation ────────────────────────────────────────────────────────

const refreshToken = process.env.RING_REFRESH_TOKEN
if (!refreshToken) {
  console.error('RING_REFRESH_TOKEN is not set in .env')
  process.exit(1)
}

const DASHBOARD_API_KEY = process.env.DASHBOARD_API_KEY
if (!DASHBOARD_API_KEY) {
  console.warn('[warn] DASHBOARD_API_KEY not set — all API endpoints are unprotected. Set this in .env.')
}

// Validate Brivo admin ID format at startup
const BRIVO_ADMIN_ID_RAW = process.env.BRIVO_ADMIN_ID
if (BRIVO_ADMIN_ID_RAW && !/^[\w\-\.@]+$/.test(BRIVO_ADMIN_ID_RAW)) {
  console.error('BRIVO_ADMIN_ID contains unexpected characters — refusing to start.')
  process.exit(1)
}

// ── Ring API ──────────────────────────────────────────────────────────────────

const ringApi = new RingApi({ refreshToken, debug: false })

// ── SSE clients ───────────────────────────────────────────────────────────────

const MAX_SSE_CLIENTS = 10
const sseClients = new Set<Response>()

function broadcastMotionSnapshot(cameraId: number, cameraName: string, jpegBase64: string) {
  const payload = JSON.stringify({ cameraId, cameraName, jpegBase64, at: new Date().toISOString() })
  for (const client of sseClients) {
    client.write(`data: ${payload}\n\n`)
  }
}

// ── Motion subscription ───────────────────────────────────────────────────────

async function subscribeMotion() {
  const cameras = await ringApi.getCameras()
  console.log(`Subscribing to motion on ${cameras.length} camera(s)…`)
  for (const camera of cameras) {
    camera.onMotionDetected.subscribe(async (motion) => {
      if (!motion) return
      console.log(`Motion on ${camera.name} — fetching snapshot…`)
      try {
        const snapshot = await camera.getSnapshot()
        broadcastMotionSnapshot(camera.id, camera.name, snapshot.toString('base64'))
      } catch (e: any) {
        console.error(`Snapshot failed for ${camera.name}: ${e.message}`)
      }
    })
  }
}

subscribeMotion().catch((e) => console.error('Motion subscription failed:', e.message))

// ── SSE keepalive (prune stale connections) ───────────────────────────────────

setInterval(() => {
  for (const client of sseClients) {
    try {
      client.write(': keepalive\n\n')
    } catch {
      sseClients.delete(client)
    }
  }
}, 30_000)

// ── Brivo auth ────────────────────────────────────────────────────────────────

const BRIVO_BASE = 'https://api.brivo.com/v1/api'
const BRIVO_AUTH = 'https://auth.brivo.com/oauth/token'

let brivoToken: string | null = null
let brivoTokenExpiry = 0

async function getBrivoToken(): Promise<string> {
  if (brivoToken && Date.now() < brivoTokenExpiry) return brivoToken

  const { BRIVO_API_KEY, BRIVO_USERNAME, BRIVO_PASSWORD, BRIVO_CLIENT_ID, BRIVO_CLIENT_SECRET } = process.env
  if (!BRIVO_API_KEY || !BRIVO_USERNAME || !BRIVO_PASSWORD) throw new Error('Brivo credentials not set in .env')
  if (!BRIVO_CLIENT_ID || !BRIVO_CLIENT_SECRET) throw new Error('BRIVO_CLIENT_ID and BRIVO_CLIENT_SECRET required — register an OAuth app at developer.brivo.com')

  const body = new URLSearchParams({ grant_type: 'password', username: BRIVO_USERNAME, password: BRIVO_PASSWORD })
  const basicCreds = Buffer.from(`${BRIVO_CLIENT_ID}:${BRIVO_CLIENT_SECRET}`).toString('base64')

  const res = await fetch(BRIVO_AUTH, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded', 'Authorization': `Basic ${basicCreds}`, 'api-key': BRIVO_API_KEY },
    body: body.toString(),
  })

  if (!res.ok) throw new Error(`Brivo auth failed ${res.status}`)

  const data: any = await res.json()
  brivoToken = data.access_token
  brivoTokenExpiry = Date.now() + (data.expires_in - 60) * 1000
  return brivoToken!
}

async function brivoGet(path: string): Promise<any> {
  const token = await getBrivoToken()
  const res = await fetch(`${BRIVO_BASE}${path}`, {
    headers: { 'Authorization': `Bearer ${token}`, 'api-key': process.env.BRIVO_API_KEY! },
  })
  if (!res.ok) throw new Error(`Brivo request failed with status ${res.status}`)
  return res.json()
}

// ── Express app ───────────────────────────────────────────────────────────────

const app = express()

// CRIT-2: CORS — restrict to localhost only
app.use(cors({ origin: [`http://localhost:${PORT}`, `http://127.0.0.1:${PORT}`] }))

// MED-3: Security headers
app.use(helmet({ contentSecurityPolicy: { directives: { defaultSrc: ["'self'"], scriptSrc: ["'self'", "'unsafe-inline'", 'cdn.jsdelivr.net'], connectSrc: ["'self'"] } } }))

// CRIT-3: Rate limiting
const generalLimiter = rateLimit({ windowMs: 60_000, max: 60, standardHeaders: true, legacyHeaders: false })
const snapshotLimiter = rateLimit({ windowMs: 60_000, max: 10, standardHeaders: true, legacyHeaders: false, message: { error: 'Too many snapshot requests' } })

app.use('/api', generalLimiter)
app.use('/api/cameras/:id/snapshot', snapshotLimiter)

// CRIT-1: Auth middleware — Bearer token check
function requireAuth(req: express.Request, res: express.Response, next: express.NextFunction) {
  if (!DASHBOARD_API_KEY) return next() // warn-only if key not configured
  const auth = req.headers.authorization
  if (!auth || auth !== `Bearer ${DASHBOARD_API_KEY}`) {
    res.status(401).json({ error: 'Unauthorized' })
    return
  }
  next()
}

// Static files — no auth required (serves dashboard.html)
app.use(express.static(join(__dirname, 'public')))

// All API routes require auth
app.use('/api', requireAuth)

// ── Helpers ───────────────────────────────────────────────────────────────────

// HIGH-3: Never expose internal error details to client
function serverError(res: express.Response, e: any) {
  console.error('[error]', e?.message || e)
  res.status(500).json({ error: 'Internal server error' })
}

// ── Routes ────────────────────────────────────────────────────────────────────

app.get('/api/status', (_req, res) => res.json({ ok: true }))

app.get('/api/motion-stream', requireAuth, (req, res) => {
  if (sseClients.size >= MAX_SSE_CLIENTS) {
    res.status(503).json({ error: 'Too many SSE connections' })
    return
  }
  res.set({ 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', 'Connection': 'keep-alive', 'X-Accel-Buffering': 'no' })
  res.flushHeaders()
  res.write(': connected\n\n')
  sseClients.add(res)
  req.on('close', () => sseClients.delete(res))
})

app.get('/api/locations', async (_req, res) => {
  try {
    const locations = await ringApi.getLocations()
    const data = await Promise.all(locations.map(async (loc) => ({
      id: loc.id,
      name: loc.name,
      cameraCount: loc.cameras.length,
      deviceCount: await loc.getDevices().then(d => d.length).catch(() => 0),
    })))
    res.json(data)
  } catch (e) { serverError(res, e) }
})

app.get('/api/cameras', async (_req, res) => {
  try {
    const cameras = await ringApi.getCameras()
    res.json(cameras.map((c) => ({
      id: c.id, name: c.name, type: c.deviceType, batteryLevel: c.batteryLevel,
      isOnline: c.data.led_status !== 'off', hasLight: c.hasLight, hasSiren: c.hasSiren,
      location: c.data.description?.location_id,
    })))
  } catch (e) { serverError(res, e) }
})

app.get('/api/cameras/:id/events', async (req, res) => {
  try {
    const cameras = await ringApi.getCameras()
    const camera = cameras.find((c) => String(c.id) === req.params.id)
    if (!camera) { res.status(404).json({ error: 'Camera not found' }); return }
    const { events } = await camera.getEvents({ limit: 20 })
    res.json(events.map((e) => ({ id: e.ding_id_str, kind: e.kind, createdAt: e.created_at, recordingStatus: e.recording_status, answered: e.answered })))
  } catch (e) { serverError(res, e) }
})

app.get('/api/cameras/:id/snapshot', async (req, res) => {
  try {
    const cameras = await ringApi.getCameras()
    const camera = cameras.find((c) => String(c.id) === req.params.id)
    if (!camera) { res.status(404).json({ error: 'Camera not found' }); return }
    const snapshot = await camera.getSnapshot()
    res.set('Content-Type', 'image/jpeg').set('Cache-Control', 'no-store').send(snapshot)
  } catch (e) { serverError(res, e) }
})

app.get('/api/locations/:id/devices', async (req, res) => {
  try {
    const locations = await ringApi.getLocations()
    const location = locations.find((l) => String(l.id) === req.params.id)
    if (!location) { res.status(404).json({ error: 'Location not found' }); return }
    const devices = await location.getDevices()
    res.json(devices.map((d) => ({ zid: d.zid, name: d.name, type: d.deviceType, batteryLevel: (d.data as any).batteryLevel ?? null, tamperStatus: (d.data as any).tamperStatus ?? null })))
  } catch (e) { serverError(res, e) }
})

app.get('/api/brivo/events', async (req, res) => {
  try {
    const limit = Math.min(Number(req.query.limit) || 50, 100)
    const path = BRIVO_ADMIN_ID_RAW
      ? `/administrators/${encodeURIComponent(BRIVO_ADMIN_ID_RAW)}/events?pageSize=${limit}&pageNo=0`
      : `/events?pageSize=${limit}&pageNo=0`
    res.json(await brivoGet(path))
  } catch (e) { serverError(res, e) }
})

app.get('/api/brivo/sites', async (_req, res) => {
  try { res.json(await brivoGet('/sites?pageSize=100&pageNo=0')) } catch (e) { serverError(res, e) }
})

app.get('/api/brivo/sites/:siteId/doors', async (req, res) => {
  try { res.json(await brivoGet(`/sites/${encodeURIComponent(req.params.siteId)}/doors?pageSize=100&pageNo=0`)) } catch (e) { serverError(res, e) }
})

app.listen(PORT, '127.0.0.1', () => {
  console.log(`Ring dashboard running at http://localhost:${PORT}`)
  console.log(`Auth: ${DASHBOARD_API_KEY ? 'enabled (Bearer token required)' : 'DISABLED — set DASHBOARD_API_KEY in .env'}`)
})
