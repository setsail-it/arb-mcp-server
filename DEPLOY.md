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
     - `DATAFORSEO_API_KEY` - Your DataForSEO Base64 authorization key (format: Base64 encoded "username:password")
     - `DATABASE_URL` - Your PostgreSQL database connection string (e.g., `postgresql://user:password@host:port/dbname`)
     - `GOOGLE_API_KEY` - Your Google API key (required for image generation via Gemini)
     - `AWS_ACCESS_KEY_ID` - Your AWS access key ID (required for S3 image hosting)
     - `AWS_SECRET_ACCESS_KEY` - Your AWS secret access key (required for S3 image hosting)
     - `AWS_S3_BUCKET` - Your S3 bucket name (defaults to "arb-imgs" if not set)
     - `AWS_REGION` - AWS region (defaults to "us-east-2" if not set)
   - Optionally set `PORT` (defaults to 8000 if not set)

4. **Access the MCP endpoint**
   - Railway exposes the HTTP endpoint at: `https://<your-project>.up.railway.app/mcp`
   - Replace `<your-project>` with your actual Railway project name

5. **Use in Prompt Playground**
   - Go to Prompt Playground → Tools → Add MCP server
   - Enter the Railway URL: `https://<your-project>.up.railway.app/mcp`

