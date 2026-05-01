# Bipolar Early Warning — Backend Server

## Deploy to Render.com

### Step 1 — Upload to GitHub
1. Create a free GitHub account at github.com if you don't have one
2. Create a new repository called `bipolar-backend`
3. Upload server.js and package.json to that repository

### Step 2 — Connect to Render
1. Go to render.com and sign in
2. Click "New" → "Web Service"
3. Connect your GitHub account and select the `bipolar-backend` repository
4. Settings:
   - Name: bipolar-early-warning-backend
   - Runtime: Node
   - Build Command: npm install
   - Start Command: node server.js
   - Plan: Free

### Step 3 — Set Environment Variables
In Render dashboard → Environment, add:
- OURA_CLIENT_ID = (your Oura client ID)
- OURA_CLIENT_SECRET = (your Oura client secret)
- REDIRECT_URI = https://bipolarrelapseearlywarningapp.netlify.app/callback
- CLINICIAN_PASSWORD = (choose a secure password)

### Step 4 — Get your server URL
Render will give you a URL like: https://bipolar-early-warning-backend.onrender.com
Copy this URL — you'll need it for the dashboard.

## API Endpoints

GET  /health                          — Check server is running
GET  /patients?password=XXX           — Get all patient data for dashboard
POST /patients/:id?password=XXX       — Add/update patient clinical profile
GET  /patients/:id/enroll-link?password=XXX — Get Oura authorization link for patient
GET  /callback                        — Oura OAuth callback (automatic)
POST /refresh                         — Refresh all patients' data (nightly)
