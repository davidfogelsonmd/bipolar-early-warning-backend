// ─────────────────────────────────────────────────────────────────────────────
// Bipolar Early Warning System — Backend Server
// Handles Oura OAuth, nightly data ingestion, and algorithm computation
// Deploy on Render.com — uses Upstash Redis for persistent storage
// ─────────────────────────────────────────────────────────────────────────────

const express = require('express');
const cors    = require('cors');
const axios   = require('axios');
const { Redis } = require('@upstash/redis');

const app  = express();
const PORT = process.env.PORT || 3000;

// ── Environment variables (set in Render dashboard) ──────────────────────────
const OURA_CLIENT_ID     = process.env.OURA_CLIENT_ID;
const OURA_CLIENT_SECRET = process.env.OURA_CLIENT_SECRET;
const REDIRECT_URI       = process.env.REDIRECT_URI || 'https://bipolar-early-warning-backend.onrender.com/callback';
const CLINICIAN_PASSWORD = process.env.CLINICIAN_PASSWORD || 'fogelson2026';

// ── Upstash Redis client ──────────────────────────────────────────────────────
const redis = new Redis({
  url:   process.env.UPSTASH_REDIS_REST_URL,
  token: process.env.UPSTASH_REDIS_REST_TOKEN,
});

// ── Middleware ────────────────────────────────────────────────────────────────
app.use(cors({ origin: '*' }));
app.use(express.json());

// ── Storage functions ─────────────────────────────────────────────────────────
async function loadPatients() {
  try {
    var data = await redis.get('patients');
    if (!data) return {};
    var raw = typeof data === 'string' ? JSON.parse(data) : data;
    // Sanitize keys
    var clean = {};
    Object.keys(raw).forEach(function(k) {
      var cleanKey = k.replace(/[^a-zA-Z0-9_-]/g, '');
      if (cleanKey && cleanKey.length > 0) {
        clean[cleanKey] = raw[k];
        clean[cleanKey].id = cleanKey;
      }
    });
    return clean;
  } catch(e) {
    console.error('Redis load error:', e.message);
    return {};
  }
}

async function savePatients(data) {
  try {
    await redis.set('patients', JSON.stringify(data));
    console.log('Saved patients to Redis:', Object.keys(data).length, 'patients');
  } catch(e) {
    console.error('Redis save error:', e.message);
  }
}

// ── ALGORITHM v3 (server-side) ────────────────────────────────────────────────
function clamp(v, lo, hi) { return Math.max(lo, Math.min(hi, v)); }

function activityScore(actArr, baseline, direction) {
  var threshold = baseline * 0.25;
  var valid = actArr.filter(v => v !== null);
  if (valid.length < 3) return 0;

  var totalDays = 0;
  for (var i = 0; i < actArr.length; i++) {
    if (actArr[i] === null) continue;
    var dev = (actArr[i] - baseline) * direction;
    if (dev > threshold) totalDays++;
  }
  var freqScore = totalDays <= 1 ? 0 : totalDays <= 3 ? 0.33 : totalDays <= 5 ? 0.66 : 1.0;

  var maxStreak = 0, streakScore = 0, i = 0;
  while (i < actArr.length) {
    if (actArr[i] === null) { i++; continue; }
    var dev = (actArr[i] - baseline) * direction;
    if (dev > threshold) {
      var streakLen = 1, worsening = true, j = i + 1;
      while (j < actArr.length) {
        if (actArr[j] === null) break;
        var devJ = (actArr[j] - baseline) * direction;
        if (devJ > threshold) {
          var devPrev = actArr[j-1] !== null ? (actArr[j-1] - baseline) * direction : 0;
          if (devJ <= devPrev) worsening = false;
          streakLen++; j++;
        } else break;
      }
      if (streakLen > maxStreak) maxStreak = streakLen;
      var sScore = streakLen <= 1 ? 0 : streakLen === 2 ? 0.40 : streakLen === 3 ? 0.70 : streakLen === 4 ? 0.85 : 1.0;
      if (worsening && streakLen >= 3) sScore = Math.min(1.0, sScore * 1.20);
      if (sScore > streakScore) streakScore = sScore;
      i = j;
    } else { i++; }
  }
  return Math.max(freqScore, streakScore);
}

function validAvg(arr, indices) {
  var sum = 0, n = 0;
  for (var i = 0; i < indices.length; i++) {
    var v = arr[indices[i]];
    if (v !== null && v !== undefined) { sum += v; n++; }
  }
  return n > 0 ? sum / n : null;
}

function devScore(val, base, dir, maxDev) {
  if (val === null || val === undefined) return 0;
  return clamp((val - base) * dir / maxDev, 0, 1);
}

function computeRisk(p) {
  var b = p.baseline;
  var valid = (p.activity || []).filter(v => v !== null).length;
  if (valid < 3) return { maniaScore: null, depScore: null, maniaTrend: 'unknown', depTrend: 'unknown' };

  var actDep  = activityScore(p.activity, b.activity, -1);
  var actMania = activityScore(p.activity, b.activity,  1);

  function scoreWindow(indices) {
    var sleep = validAvg(p.sleep,    indices);
    var hrv   = validAvg(p.hrv,      indices);
    var hr    = validAvg(p.hr,       indices);
    var eff   = validAvg(p.sleepEff || [], indices);
    if (sleep === null || hrv === null || hr === null) return { maniaRaw: 0, depRaw: 0 };

    var circ = p.circadianShift || 0;
    var temp = p.tempDev || 0;
    var resp = p.respRate || b.resp;
    var tempDev = Math.abs(temp);

    // Sleep efficiency
    var mEff = 0, dEff = 0;
    if (eff !== null) {
      dEff = eff < 55 ? 1.0 : eff < 65 ? 0.66 : eff < 75 ? 0.33 : 0;
      var sleepShort = sleep < 7.0;
      var effAbove   = eff > (b.eff || 82);
      mEff = (sleepShort && effAbove) ? clamp((eff - (b.eff || 82)) / 15, 0, 1) : 0;
    }

    // Mania
    var mSleep = devScore(sleep, b.sleep, -1, 3.0) * 30;
    var mCirc  = devScore(circ,  0,       -1, 2.0) * 25;
    var mAct   = actMania * 20;
    var mHR    = devScore(hr,    b.hr,     1, 20)  * 12;
    var mResp  = devScore(resp,  b.resp,   1, 6)   * 7;
    var mTemp  = clamp(tempDev / 1.0, 0, 1) * 5;
    var maniaRaw = mSleep + mCirc + mAct + mHR + mResp + mTemp + (mEff * 8);

    // Depression
    var dAct    = actDep * 35;
    var dCirc   = devScore(circ, 0,      1, 2.5) * 20;
    var dHRV    = devScore(hrv,  b.hrv, -1, 25)  * 15;
    var dSleepU = devScore(sleep, b.sleep, 1, 1.5);
    var dSleepD = devScore(sleep, b.sleep,-1, 1.5);
    var dSleep  = Math.max(dSleepU, dSleepD) * 12;
    var dHR     = devScore(hr,   b.hr,   1, 15)  * 10;
    var dTemp   = clamp(tempDev / 1.0, 0, 1) * 5;
    var depRaw  = dAct + dCirc + dHRV + dSleep + dHR + dTemp + (dEff * 8);

    return { maniaRaw, depRaw };
  }

  var recent  = scoreWindow([4, 5, 6]);
  var earlier = scoreWindow([0, 1, 2, 3]);

  var maniaScore = Math.round(clamp(recent.maniaRaw * 0.6 + earlier.maniaRaw * 0.4, 0, 100));
  var depScore   = Math.round(clamp(recent.depRaw   * 0.6 + earlier.depRaw   * 0.4, 0, 100));

  // Mania anchor floor
  var rSleep = validAvg(p.sleep, [4,5,6]);
  var rSleepDrop = rSleep !== null ? (b.sleep - rSleep) / b.sleep : 0;
  var dualDays = 0;
  for (var d = 6; d >= 0; d--) {
    if (!p.sleep[d] || !p.activity[d]) break;
    if ((b.sleep - p.sleep[d]) / b.sleep >= 0.15 && (p.activity[d] - b.activity) / b.activity >= 0.25) dualDays++;
    else break;
  }
  if (rSleepDrop >= 0.15 && actMania >= 0.60) maniaScore = Math.max(maniaScore, 82);
  else if (dualDays >= 2 && rSleepDrop >= 0.10) maniaScore = Math.max(maniaScore, 75);
  else if (dualDays >= 1 && rSleepDrop >= 0.08) maniaScore = Math.max(maniaScore, 65);

  // Depression anchor floor
  var rHrv = validAvg(p.hrv, [4,5,6]);
  var actSevere  = actDep >= 0.70;
  var sleepSev   = rSleep !== null && Math.abs(rSleep - b.sleep) / b.sleep >= 0.15;
  var hrvSev     = rHrv !== null && devScore(rHrv, b.hrv, -1, 25) >= 0.60;
  if ((actSevere ? 1 : 0) + (sleepSev ? 1 : 0) + (hrvSev ? 1 : 0) >= 2) depScore = Math.max(depScore, 75);

  var maniaDelta = recent.maniaRaw - earlier.maniaRaw;
  var depDelta   = recent.depRaw   - earlier.depRaw;
  var maniaTrend = maniaDelta > 5 ? 'rising' : maniaDelta < -5 ? 'falling' : 'stable';
  var depTrend   = depDelta   > 5 ? 'rising' : depDelta   < -5 ? 'falling' : 'stable';

  return { maniaScore, depScore, maniaTrend, depTrend };
}

// ── OURA DATA PARSER ──────────────────────────────────────────────────────────
// Extracts the 7 signals we need from Oura API responses
function parseOuraData(sleepData, activityData, days) {
  // Build date-indexed lookup
  var sleepByDate    = {};
  var activityByDate = {};

  (sleepData || []).forEach(s => {
    var date = s.day || s.date;
    if (!date) return;
    sleepByDate[date] = {
      total_sleep: s.total_sleep_duration ? s.total_sleep_duration / 3600 : null, // seconds → hours
      efficiency:  s.efficiency || null,
      latency:     s.sleep_phase_5_min ? null : null,
      hr:          s.average_heart_rate || null,
      hrv:         s.average_hrv || null,
      breath:      s.average_breath || null,
      temp:        s.temperature_deviation || null,
      onset:       s.bedtime_start || null,
    };
  });

  (activityData || []).forEach(a => {
    var date = a.day || a.date;
    if (!date) return;
    activityByDate[date] = {
      steps: a.steps || null,
    };
  });

  // Build 7-day arrays in order
  var sleep    = days.map(d => sleepByDate[d]    ? sleepByDate[d].total_sleep : null);
  var sleepEff = days.map(d => sleepByDate[d]    ? sleepByDate[d].efficiency  : null);
  var hrv      = days.map(d => sleepByDate[d]    ? sleepByDate[d].hrv         : null);
  var hr       = days.map(d => sleepByDate[d]    ? sleepByDate[d].hr          : null);
  var activity = days.map(d => activityByDate[d] ? activityByDate[d].steps    : null);
  var tempDev  = sleepByDate[days[days.length-1]] ? sleepByDate[days[days.length-1]].temp : 0;
  var respRate = sleepByDate[days[days.length-1]] ? sleepByDate[days[days.length-1]].breath : 14;

  return { sleep, sleepEff, hrv, hr, activity, tempDev, respRate };
}

// ── ROUTES ────────────────────────────────────────────────────────────────────

// Redis connection test
app.get('/test-redis', async (req, res) => {
  try {
    await redis.set('test', 'hello');
    var val = await redis.get('test');
    res.json({ success: true, value: val, url_set: !!process.env.UPSTASH_REDIS_REST_URL, token_set: !!process.env.UPSTASH_REDIS_REST_TOKEN });
  } catch(e) {
    res.json({ success: false, error: e.message, url_set: !!process.env.UPSTASH_REDIS_REST_URL, token_set: !!process.env.UPSTASH_REDIS_REST_TOKEN });
  }
});

// Health check
app.get('/health', (req, res) => {
  res.json({ status: 'ok', service: 'Bipolar Early Warning Backend', version: '1.0' });
});

// Step 1: Generate Oura authorization URL for a patient
app.get('/enroll', (req, res) => {
  var patientId = req.query.patient_id;
  if (!patientId) return res.status(400).json({ error: 'patient_id required' });

  var scope = 'personal daily heartrate workout tag session spo2 sleep';
  var cleanId = patientId.replace(/[^a-zA-Z0-9_-]/g, '');
  var url = `https://cloud.ouraring.com/oauth/authorize`
    + `?response_type=code`
    + `&client_id=${OURA_CLIENT_ID}`
    + `&redirect_uri=${encodeURIComponent(REDIRECT_URI)}`
    + `&scope=${encodeURIComponent(scope)}`
    + `&state=${encodeURIComponent(cleanId)}`;

  res.json({ authorization_url: url });
});

// Step 2: Handle OAuth callback from Oura
app.get('/callback', async (req, res) => {
  var { code, state } = req.query;
  // Sanitize patient ID — remove any extra characters
  var patientId = state ? state.toString().replace(/[^a-zA-Z0-9_-]/g, '') : null;
  if (!code || !patientId) return res.status(400).send('Missing code or patient ID');

  try {
    // Exchange code for tokens — must use form-encoded body
    var params = new URLSearchParams();
    params.append('grant_type',    'authorization_code');
    params.append('code',          code);
    params.append('redirect_uri',  REDIRECT_URI);
    params.append('client_id',     OURA_CLIENT_ID);
    params.append('client_secret', OURA_CLIENT_SECRET);

    var tokenRes = await axios.post('https://api.ouraring.com/oauth/token', params, {
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' }
    });

    var { access_token, refresh_token } = tokenRes.data;

    // Store tokens for this patient
    var patients = await loadPatients();
    if (!patients[patientId]) {
      patients[patientId] = {
        id: patientId,
        name: 'Patient ' + patientId,
        baseline: { sleep: 7.0, hrv: 35, activity: 10000, hr: 54, resp: 14, eff: 85 }
      };
    }
    patients[patientId].access_token  = access_token;
    patients[patientId].refresh_token = refresh_token;
    patients[patientId].connected     = true;
    patients[patientId].connected_at  = new Date().toISOString();
    await savePatients(patients);

    // Fetch initial data
    await fetchAndUpdatePatient(patientId, access_token, patients);
    await savePatients(patients);

    res.send(`
      <html><body style="font-family:sans-serif;text-align:center;padding:3rem;">
        <h2>✓ Connected successfully</h2>
        <p>Your Oura Ring is now connected to Dr. Fogelson's monitoring system.</p>
        <p>Your data will be reviewed daily. You may close this window.</p>
      </body></html>
    `);
  } catch(e) {
    console.error('OAuth error:', e.response?.data || e.message);
    res.status(500).send('Connection failed — please contact Dr. Fogelson\'s office.');
  }
});

// Fetch and compute risk for a patient
async function fetchAndUpdatePatient(patientId, accessToken, patients) {
  try {
    // Get last 8 days of data (7 for algorithm + 1 buffer)
    var endDate   = new Date();
    var startDate = new Date();
    startDate.setDate(endDate.getDate() - 8);

    var start = startDate.toISOString().split('T')[0];
    var end   = endDate.toISOString().split('T')[0];

    var headers = { Authorization: `Bearer ${accessToken}` };

    var [sleepRes, actRes] = await Promise.all([
      axios.get(`https://api.ouraring.com/v2/usercollection/sleep?start_date=${start}&end_date=${end}`, { headers }),
      axios.get(`https://api.ouraring.com/v2/usercollection/daily_activity?start_date=${start}&end_date=${end}`, { headers }),
    ]);

    // Build 7-day date array
    var days = [];
    for (var i = 6; i >= 0; i--) {
      var d = new Date();
      d.setDate(d.getDate() - i);
      days.push(d.toISOString().split('T')[0]);
    }

    var parsed = parseOuraData(sleepRes.data.data, actRes.data.data, days);

    // Merge with patient record
    var p = patients[patientId];
    p.sleep       = parsed.sleep;
    p.sleepEff    = parsed.sleepEff;
    p.hrv         = parsed.hrv;
    p.hr          = parsed.hr;
    p.activity    = parsed.activity;
    p.tempDev     = parsed.tempDev;
    p.respRate    = parsed.respRate || 14;
    p.days        = days;
    p.last_sync   = new Date().toISOString();

    // Compute circadian shift from sleep onset vs personal baseline onset
    var lastSleep = sleepRes.data.data.slice(-1)[0];
    if (lastSleep && lastSleep.bedtime_start && p.baseline && p.baseline.bedtime_start) {
      var baseHour = parseFloat(p.baseline.bedtime_start);
      var currHour = new Date(lastSleep.bedtime_start).getHours() + new Date(lastSleep.bedtime_start).getMinutes() / 60;
      p.circadianShift = currHour - baseHour;
    } else {
      p.circadianShift = 0;
    }

    // Run algorithm
    if (p.baseline) {
      var risk = computeRisk(p);
      p.maniaRisk  = risk.maniaScore;
      p.depRisk    = risk.depScore;
      p.maniaTrend = risk.maniaTrend;
      p.depTrend   = risk.depTrend;
      p.status     = Math.max(p.maniaRisk || 0, p.depRisk || 0) >= 75 ? 'high'
                   : Math.max(p.maniaRisk || 0, p.depRisk || 0) >= 50 ? 'warn' : 'ok';
    }

  } catch(e) {
    console.error(`Fetch error for ${patientId}:`, e.response?.data || e.message);
  }
}

// Step 3: Refresh all patients' data (call nightly via cron)
app.post('/refresh', async (req, res) => {
  var { password } = req.body;
  if (password !== CLINICIAN_PASSWORD) return res.status(401).json({ error: 'Unauthorized' });

  var patients = await loadPatients();
  var results  = [];

  for (var id of Object.keys(patients)) {
    var p = patients[id];
    if (!p.access_token) continue;
    await fetchAndUpdatePatient(id, p.access_token, patients);
    results.push({ id, status: p.status, maniaRisk: p.maniaRisk, depRisk: p.depRisk });
  }

  await savePatients(patients);
  res.json({ updated: results.length, patients: results });
});

// Step 4: Get all patient data for the dashboard
app.get('/patients', async (req, res) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET');
  var { password } = req.query;
  if (password !== CLINICIAN_PASSWORD) return res.status(401).json({ error: 'Unauthorized' });

  var patients = await loadPatients();
  var result   = Object.values(patients).map(p => ({
    id:              p.id,
    name:            p.name || 'Patient ' + p.id,
    age:             p.age,
    gender:          p.gender,
    dx:              p.dx,
    predominantPolarity: p.predominantPolarity,
    lastManiaDate:   p.lastManiaDate,
    lastDepDate:     p.lastDepDate,
    typicalProdrome: p.typicalProdrome,
    maniaRisk:       p.maniaRisk,
    depRisk:         p.depRisk,
    maniaTrend:      p.maniaTrend,
    depTrend:        p.depTrend,
    status:          p.status,
    connected:       p.connected || false,
    last_sync:       p.last_sync,
    sleep:           p.sleep,
    sleepEff:        p.sleepEff,
    hrv:             p.hrv,
    hr:              p.hr,
    activity:        p.activity,
    tempDev:         p.tempDev,
    respRate:        p.respRate,
    circadianShift:  p.circadianShift,
    days:            p.days,
    baseline:        p.baseline,
  }));

  res.json({ patients: result, last_updated: new Date().toISOString() });
});

// Step 5: Add or update patient clinical profile
app.post('/patients/:id', async (req, res) => {
  var { password } = req.query;
  if (password !== CLINICIAN_PASSWORD) return res.status(401).json({ error: 'Unauthorized' });

  var patients = await loadPatients();
  var id       = req.params.id;
  if (!patients[id]) patients[id] = { id };

  var allowed = ['name','age','gender','dx','predominantPolarity','lastManiaDate',
                 'lastDepDate','lastDepPolarity','typicalProdrome','baseline'];
  allowed.forEach(k => { if (req.body[k] !== undefined) patients[id][k] = req.body[k]; });

  await savePatients(patients);
  res.json({ success: true, patient: patients[id] });
});

// Step 6: Get patient enrollment link
app.get('/patients/:id/enroll-link', (req, res) => {
  var { password } = req.query;
  if (password !== CLINICIAN_PASSWORD) return res.status(401).json({ error: 'Unauthorized' });

  var scope = 'personal daily heartrate workout tag session spo2 sleep';
  var cleanId = req.params.id.replace(/[^a-zA-Z0-9_-]/g, '');
  var url = `https://cloud.ouraring.com/oauth/authorize`
    + `?response_type=code`
    + `&client_id=${OURA_CLIENT_ID}`
    + `&redirect_uri=${encodeURIComponent(REDIRECT_URI)}`
    + `&scope=${encodeURIComponent(scope)}`
    + `&state=${encodeURIComponent(cleanId)}`;

  res.json({ enrollment_url: url });
});

app.listen(PORT, () => {
  console.log(`Bipolar Early Warning backend running on port ${PORT}`);
});
