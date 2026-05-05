var express = require('express');
var cors = require('cors');
var axios = require('axios');
var Redis = require('@upstash/redis').Redis;

var app = express();
var PORT = process.env.PORT || 3000;

var OURA_CLIENT_ID = process.env.OURA_CLIENT_ID;
var OURA_CLIENT_SECRET = process.env.OURA_CLIENT_SECRET;
var REDIRECT_URI = process.env.REDIRECT_URI || 'https://bipolar-early-warning-backend.onrender.com/callback';
var CLINICIAN_PASSWORD = process.env.CLINICIAN_PASSWORD || 'fogelson2026';
var CRON_SECRET = process.env.CRON_SECRET || 'cron2026';

var redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL,
  token: process.env.UPSTASH_REDIS_REST_TOKEN,
});

app.use(cors({ origin: '*' }));
app.use(express.json());

// ── STORAGE ───────────────────────────────────────────────────────────────────
async function loadPatients() {
  try {
    var data = await redis.get('patients');
    if (!data) return {};
    var raw = typeof data === 'string' ? JSON.parse(data) : data;
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
    console.log('Saved ' + Object.keys(data).length + ' patients to Redis');
  } catch(e) {
    console.error('Redis save error:', e.message);
  }
}

// ── QUEUE SYSTEM ──────────────────────────────────────────────────────────────
// Each patient refresh is an independent job stored in Redis
// This allows future distribution across multiple workers
// Queue key: 'refresh_queue' — list of patient IDs waiting to be refreshed
// Job key: 'job:{patientId}' — individual job status and result
// At scale: replace with proper queue (BullMQ, SQS, etc.) without changing API

async function enqueueRefresh(patientIds) {
  try {
    // Store refresh jobs as individual Redis keys with TTL
    for (var i = 0; i < patientIds.length; i++) {
      var jobKey = 'job:' + patientIds[i];
      await redis.set(jobKey, JSON.stringify({
        patientId: patientIds[i],
        status: 'pending',
        enqueued_at: new Date().toISOString()
      }));
      await redis.expire(jobKey, 3600); // expire after 1 hour
    }
    // Store queue as a list
    await redis.set('refresh_queue', JSON.stringify(patientIds));
    console.log('Enqueued ' + patientIds.length + ' patients for refresh');
  } catch(e) {
    console.error('Queue error:', e.message);
  }
}

async function processRefreshQueue() {
  var patients = await loadPatients();
  var ids = Object.keys(patients);
  var results = [];
  var errors = [];

  console.log('Processing refresh queue: ' + ids.length + ' patients');

  // Process patients in batches of 10 to avoid rate limiting
  // At scale: each batch becomes a separate worker job
  var batchSize = 10;
  for (var i = 0; i < ids.length; i += batchSize) {
    var batch = ids.slice(i, i + batchSize);
    var batchPromises = batch.map(function(id) {
      return refreshPatient(id, patients).then(function(result) {
        results.push(result);
        return result;
      }).catch(function(e) {
        errors.push({ id: id, error: e.message });
        console.error('Error refreshing ' + id + ':', e.message);
      });
    });
    // Process batch concurrently, then wait before next batch
    await Promise.all(batchPromises);
    if (i + batchSize < ids.length) {
      // Small delay between batches to respect API rate limits
      await new Promise(function(resolve) { setTimeout(resolve, 1000); });
    }
  }

  await savePatients(patients);

  // Update queue status
  var queueStatus = {
    last_run: new Date().toISOString(),
    patients_refreshed: results.length,
    errors: errors.length,
    next_run: getNextRunTime()
  };
  await redis.set('queue_status', JSON.stringify(queueStatus));

  return { refreshed: results.length, errors: errors.length, results: results };
}

async function refreshPatient(id, patients) {
  var p = patients[id];
  if (!p || !p.access_token) {
    return { id: id, status: 'skipped', reason: 'no access token' };
  }

  try {
    // Update job status
    var jobKey = 'job:' + id;
    await redis.set(jobKey, JSON.stringify({ patientId: id, status: 'running', started_at: new Date().toISOString() }));
    await redis.expire(jobKey, 3600);

    await fetchAndUpdatePatient(id, p.access_token, patients);

    // Mark job complete
    await redis.set(jobKey, JSON.stringify({
      patientId: id, status: 'complete',
      completed_at: new Date().toISOString(),
      maniaRisk: p.maniaRisk, depRisk: p.depRisk, status_risk: p.status
    }));
    await redis.expire(jobKey, 3600);

    return { id: id, status: 'ok', maniaRisk: p.maniaRisk, depRisk: p.depRisk };
  } catch(e) {
    await redis.set(jobKey, JSON.stringify({ patientId: id, status: 'error', error: e.message }));
    await redis.expire(jobKey, 3600);
    throw e;
  }
}

function getNextRunTime() {
  // Refresh every 3 hours: 6am, 9am, 12pm, 3pm, 6pm, 9pm
  var now = new Date();
  var hours = [6, 9, 12, 15, 18, 21];
  var nextHour = hours.find(function(h) { return h > now.getHours(); });
  if (!nextHour) nextHour = hours[0]; // wrap to next day
  var next = new Date(now);
  next.setHours(nextHour, 0, 0, 0);
  if (nextHour <= now.getHours()) next.setDate(next.getDate() + 1);
  return next.toISOString();
}

// ── ALGORITHM ─────────────────────────────────────────────────────────────────
function clamp(v,lo,hi){return Math.max(lo,Math.min(hi,v));}

function activityScore(actArr, baseline, direction){
  var threshold=baseline*0.25;
  var valid=actArr.filter(function(v){return v!==null;});
  if(valid.length<3) return 0;
  var totalDays=0;
  for(var i=0;i<actArr.length;i++){
    if(actArr[i]===null) continue;
    if((actArr[i]-baseline)*direction>threshold) totalDays++;
  }
  var freqScore=totalDays<=1?0:totalDays<=3?0.33:totalDays<=5?0.66:1.0;
  var maxStreak=0,streakScore=0,i=0;
  while(i<actArr.length){
    if(actArr[i]===null){i++;continue;}
    if((actArr[i]-baseline)*direction>threshold){
      var streakLen=1,worsening=true,j=i+1;
      while(j<actArr.length){
        if(actArr[j]===null) break;
        if((actArr[j]-baseline)*direction>threshold){
          var devPrev=actArr[j-1]!==null?(actArr[j-1]-baseline)*direction:0;
          if((actArr[j]-baseline)*direction<=devPrev) worsening=false;
          streakLen++;j++;
        } else break;
      }
      if(streakLen>maxStreak) maxStreak=streakLen;
      var sScore=streakLen<=1?0:streakLen===2?0.40:streakLen===3?0.70:streakLen===4?0.85:1.0;
      if(worsening&&streakLen>=3) sScore=Math.min(1.0,sScore*1.20);
      if(sScore>streakScore) streakScore=sScore;
      i=j;
    } else {i++;}
  }
  return Math.max(freqScore,streakScore);
}

function validAvg(arr,indices){
  var sum=0,n=0;
  for(var i=0;i<indices.length;i++){
    var v=arr[indices[i]];
    if(v!==null&&v!==undefined){sum+=v;n++;}
  }
  return n>0?sum/n:null;
}

function devScore(val,base,dir,maxDev){
  if(val===null||val===undefined) return 0;
  return clamp((val-base)*dir/maxDev,0,1);
}

function computeRisk(p){
  var b=p.baseline;
  var valid=(p.activity||[]).filter(function(v){return v!==null;}).length;
  if(valid<3) return {maniaScore:null,depScore:null,maniaTrend:'unknown',depTrend:'unknown'};
  var actDep=activityScore(p.activity,b.activity,-1);
  var actMania=activityScore(p.activity,b.activity,1);
  function scoreWindow(indices){
    var sleep=validAvg(p.sleep,indices);
    var hrv=validAvg(p.hrv,indices);
    var hr=validAvg(p.hr,indices);
    var eff=validAvg(p.sleepEff||[],indices);
    if(sleep===null||hrv===null||hr===null) return {maniaRaw:0,depRaw:0};
    var circ=p.circadianShift||0,temp=p.tempDev||0,resp=p.respRate||b.resp;
    var tempDev=Math.abs(temp),mEff=0,dEff=0;
    if(eff!==null){
      dEff=eff<55?1.0:eff<65?0.66:eff<75?0.33:0;
      mEff=(sleep<7.0&&eff>(b.eff||82))?clamp((eff-(b.eff||82))/15,0,1):0;
    }
    var maniaRaw=devScore(sleep,b.sleep,-1,3.0)*30+devScore(circ,0,-1,2.0)*25+actMania*20+devScore(hr,b.hr,1,20)*12+devScore(resp,b.resp,1,6)*7+clamp(tempDev,0,1)*5+(mEff*8);
    var dSleepU=devScore(sleep,b.sleep,1,1.5),dSleepD=devScore(sleep,b.sleep,-1,1.5);
    var depRaw=actDep*35+devScore(circ,0,1,2.5)*20+devScore(hrv,b.hrv,-1,25)*15+Math.max(dSleepU,dSleepD)*12+devScore(hr,b.hr,1,15)*10+clamp(tempDev,0,1)*5+(dEff*8);
    return {maniaRaw:maniaRaw,depRaw:depRaw};
  }
  var recent=scoreWindow([4,5,6]),earlier=scoreWindow([0,1,2,3]);
  var maniaScore=Math.round(clamp(recent.maniaRaw*0.6+earlier.maniaRaw*0.4,0,100));
  var depScore=Math.round(clamp(recent.depRaw*0.6+earlier.depRaw*0.4,0,100));
  var rSleep=validAvg(p.sleep,[4,5,6]),rSleepDrop=rSleep!==null?(b.sleep-rSleep)/b.sleep:0;
  var dualDays=0;
  for(var d=6;d>=0;d--){
    if(!p.sleep[d]||!p.activity[d]) break;
    if((b.sleep-p.sleep[d])/b.sleep>=0.15&&(p.activity[d]-b.activity)/b.activity>=0.25) dualDays++;
    else break;
  }
  if(rSleepDrop>=0.15&&actMania>=0.60) maniaScore=Math.max(maniaScore,82);
  else if(dualDays>=2&&rSleepDrop>=0.10) maniaScore=Math.max(maniaScore,75);
  else if(dualDays>=1&&rSleepDrop>=0.08) maniaScore=Math.max(maniaScore,65);
  var rHrv=validAvg(p.hrv,[4,5,6]);
  if((actDep>=0.70?1:0)+(rSleep!==null&&Math.abs(rSleep-b.sleep)/b.sleep>=0.15?1:0)+(rHrv!==null&&devScore(rHrv,b.hrv,-1,25)>=0.60?1:0)>=2) depScore=Math.max(depScore,75);
  var maniaDelta=recent.maniaRaw-earlier.maniaRaw,depDelta=recent.depRaw-earlier.depRaw;
  return {
    maniaScore:maniaScore,depScore:depScore,
    maniaTrend:maniaDelta>5?'rising':maniaDelta<-5?'falling':'stable',
    depTrend:depDelta>5?'rising':depDelta<-5?'falling':'stable'
  };
}

// ── OURA DATA FETCH ───────────────────────────────────────────────────────────
function parseOuraData(sleepData,activityData,days){
  var sleepByDate={},activityByDate={};
  (sleepData||[]).forEach(function(s){
    var date=s.day||s.date;
    if(!date) return;
    sleepByDate[date]={total_sleep:s.total_sleep_duration?s.total_sleep_duration/3600:null,efficiency:s.efficiency||null,hr:s.average_heart_rate||null,hrv:s.average_hrv||null,breath:s.average_breath||null,temp:s.temperature_deviation||null,onset:s.bedtime_start||null};
  });
  (activityData||[]).forEach(function(a){
    var date=a.day||a.date;
    if(!date) return;
    activityByDate[date]={steps:a.steps||null};
  });
  var last=sleepByDate[days[days.length-1]];
  return {
    sleep:days.map(function(d){return sleepByDate[d]?sleepByDate[d].total_sleep:null;}),
    sleepEff:days.map(function(d){return sleepByDate[d]?sleepByDate[d].efficiency:null;}),
    hrv:days.map(function(d){return sleepByDate[d]?sleepByDate[d].hrv:null;}),
    hr:days.map(function(d){return sleepByDate[d]?sleepByDate[d].hr:null;}),
    activity:days.map(function(d){return activityByDate[d]?activityByDate[d].steps:null;}),
    tempDev:last?last.temp:0,
    respRate:last?last.breath:14
  };
}

async function fetchAndUpdatePatient(patientId,accessToken,patients){
  var endDate=new Date(),startDate=new Date();
  startDate.setDate(endDate.getDate()-8);
  var start=startDate.toISOString().split('T')[0],end=endDate.toISOString().split('T')[0];
  var headers={Authorization:'Bearer '+accessToken};
  var results=await Promise.all([
    axios.get('https://api.ouraring.com/v2/usercollection/sleep?start_date='+start+'&end_date='+end,{headers:headers}),
    axios.get('https://api.ouraring.com/v2/usercollection/daily_activity?start_date='+start+'&end_date='+end,{headers:headers}),
  ]);
  var days=[];
  for(var i=6;i>=0;i--){var d=new Date();d.setDate(d.getDate()-i);days.push(d.toISOString().split('T')[0]);}
  var parsed=parseOuraData(results[0].data.data,results[1].data.data,days);
  var p=patients[patientId];
  Object.assign(p,parsed);
  p.days=days;
  p.last_sync=new Date().toISOString();
  var lastSleep=results[0].data.data.slice(-1)[0];
  if(lastSleep&&lastSleep.bedtime_start&&p.baseline&&p.baseline.bedtime_start){
    var dt=new Date(lastSleep.bedtime_start);
    p.circadianShift=dt.getHours()+dt.getMinutes()/60-parseFloat(p.baseline.bedtime_start);
  } else {p.circadianShift=0;}
  if(p.baseline){
    var risk=computeRisk(p);
    p.maniaRisk=risk.maniaScore;p.depRisk=risk.depScore;
    p.maniaTrend=risk.maniaTrend;p.depTrend=risk.depTrend;
    p.status=Math.max(p.maniaRisk||0,p.depRisk||0)>=75?'high':Math.max(p.maniaRisk||0,p.depRisk||0)>=50?'warn':'ok';
  }
}

// ── ROUTES ────────────────────────────────────────────────────────────────────
app.get('/test-redis', async function(req,res){
  try {
    await redis.set('test','hello');
    var val=await redis.get('test');
    res.json({success:true,value:val,url_set:!!process.env.UPSTASH_REDIS_REST_URL,token_set:!!process.env.UPSTASH_REDIS_REST_TOKEN});
  } catch(e){res.json({success:false,error:e.message});}
});

app.get('/health', function(req,res){
  res.json({status:'ok',service:'Bipolar Early Warning Backend',version:'3.0-queue'});
});

// Queue status endpoint
app.get('/queue/status', async function(req,res){
  var password=req.query.password;
  if(password!==CLINICIAN_PASSWORD) return res.status(401).json({error:'Unauthorized'});
  try {
    var status=await redis.get('queue_status');
    res.json(status?JSON.parse(status):{message:'No refresh run yet'});
  } catch(e){res.json({error:e.message});}
});

// Scheduled refresh endpoint — called by Render cron every 3 hours
// Protected by CRON_SECRET so only Render can trigger it
app.post('/queue/run', async function(req,res){
  var secret=req.headers['x-cron-secret']||req.body.secret;
  if(secret!==CRON_SECRET) return res.status(401).json({error:'Unauthorized'});
  console.log('Scheduled refresh triggered at '+new Date().toISOString());
  try {
    var patients=await loadPatients();
    var ids=Object.keys(patients).filter(function(id){return patients[id].access_token;});
    await enqueueRefresh(ids);
    var result=await processRefreshQueue();
    res.json({success:true,triggered_at:new Date().toISOString(),result:result});
  } catch(e){
    console.error('Queue run error:',e.message);
    res.status(500).json({error:e.message});
  }
});

// Manual refresh — triggered by clinician from dashboard
app.post('/refresh', async function(req,res){
  var password=req.body.password;
  if(password!==CLINICIAN_PASSWORD) return res.status(401).json({error:'Unauthorized'});
  try {
    var result=await processRefreshQueue();
    res.json({success:true,result:result});
  } catch(e){res.status(500).json({error:e.message});}
});

app.get('/enroll', function(req,res){
  var patientId=req.query.patient_id;
  if(!patientId) return res.status(400).json({error:'patient_id required'});
  var scope='personal daily heartrate workout tag session spo2 sleep';
  var cleanId=patientId.replace(/[^a-zA-Z0-9_-]/g,'');
  var url='https://cloud.ouraring.com/oauth/authorize?response_type=code&client_id='+OURA_CLIENT_ID+'&redirect_uri='+encodeURIComponent(REDIRECT_URI)+'&scope='+encodeURIComponent(scope)+'&state='+encodeURIComponent(cleanId);
  res.json({authorization_url:url});
});

app.get('/callback', async function(req,res){
  var code=req.query.code,state=req.query.state;
  var patientId=state?state.toString().replace(/[^a-zA-Z0-9_-]/g,''):null;
  if(!code||!patientId) return res.status(400).send('Missing code or patient ID');
  try {
    var params=new URLSearchParams();
    params.append('grant_type','authorization_code');params.append('code',code);
    params.append('redirect_uri',REDIRECT_URI);params.append('client_id',OURA_CLIENT_ID);
    params.append('client_secret',OURA_CLIENT_SECRET);
    var tokenRes=await axios.post('https://api.ouraring.com/oauth/token',params,{headers:{'Content-Type':'application/x-www-form-urlencoded'}});
    var access_token=tokenRes.data.access_token,refresh_token=tokenRes.data.refresh_token;
    var patients=await loadPatients();
    if(!patients[patientId]) patients[patientId]={id:patientId,name:'Patient '+patientId,baseline:{sleep:7.0,hrv:35,activity:10000,hr:54,resp:14,eff:85}};
    patients[patientId].access_token=access_token;
    patients[patientId].refresh_token=refresh_token;
    patients[patientId].connected=true;
    patients[patientId].connected_at=new Date().toISOString();
    await fetchAndUpdatePatient(patientId,access_token,patients);
    await savePatients(patients);
    res.send('<html><body style="font-family:sans-serif;text-align:center;padding:3rem;"><h2>Connected successfully</h2><p>Your Oura Ring is now connected to Dr. Fogelson\'s monitoring system.</p><p>Your data will be reviewed daily. You may close this window.</p></body></html>');
  } catch(e){
    console.error('OAuth error:',e.response?e.response.data:e.message);
    res.status(500).send('Connection failed. Please contact Dr. Fogelson\'s office.');
  }
});

app.get('/patients', async function(req,res){
  res.header('Access-Control-Allow-Origin','*');
  var password=req.query.password;
  if(password!==CLINICIAN_PASSWORD) return res.status(401).json({error:'Unauthorized'});
  var patients=await loadPatients();
  var result=Object.values(patients).map(function(p){
    return {id:p.id,name:p.name||'Patient '+p.id,age:p.age,gender:p.gender,dx:p.dx,predominantPolarity:p.predominantPolarity,lastManiaDate:p.lastManiaDate,lastDepDate:p.lastDepDate,typicalProdrome:p.typicalProdrome,maniaRisk:p.maniaRisk,depRisk:p.depRisk,maniaTrend:p.maniaTrend,depTrend:p.depTrend,status:p.status,connected:p.connected||false,last_sync:p.last_sync,sleep:p.sleep,sleepEff:p.sleepEff,hrv:p.hrv,hr:p.hr,activity:p.activity,tempDev:p.tempDev,respRate:p.respRate,circadianShift:p.circadianShift,days:p.days,baseline:p.baseline};
  });
  res.json({patients:result,last_updated:new Date().toISOString()});
});

app.post('/patients/:id', async function(req,res){
  var password=req.query.password;
  if(password!==CLINICIAN_PASSWORD) return res.status(401).json({error:'Unauthorized'});
  var patients=await loadPatients();
  var id=req.params.id;
  if(!patients[id]) patients[id]={id:id};
  ['name','age','gender','dx','predominantPolarity','lastManiaDate','lastDepDate','lastDepPolarity','typicalProdrome','baseline'].forEach(function(k){if(req.body[k]!==undefined) patients[id][k]=req.body[k];});
  await savePatients(patients);
  res.json({success:true,patient:patients[id]});
});

app.listen(PORT,function(){console.log('Bipolar Early Warning v3 running on port '+PORT);});
