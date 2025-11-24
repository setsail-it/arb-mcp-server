# Railway Deployment Guide

## Steps to Deploy

1. **Push repo to GitHub**
   - Ensure your code is committed and pushed to a GitHub repository

2. **Create new Railway service**
   - Go to [Railway](https://railway.app)
   - Click "New Project" → "Deploy from GitHub repo"
   - Select your repository

3. **Configure environment variables**
   - In Railway dashboard, go to your service → Variables
   - Add the following environment variables:
     - `DATAFORSEO_API_KEY` - Your DataForSEO login email
     - `DATAFORSEO_API_SECRET` - Your DataForSEO API password
   - Optionally set `PORT` (defaults to 8000 if not set)

4. **Access the MCP endpoint**
   - Railway exposes the HTTP endpoint at: `https://<your-project>.up.railway.app/mcp`
   - Replace `<your-project>` with your actual Railway project name

5. **Use in Prompt Playground**
   - Go to Prompt Playground → Tools → Add MCP server
   - Enter the Railway URL: `https://<your-project>.up.railway.app/mcp`

