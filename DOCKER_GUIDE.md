Dispatcharr-Maid

Docker Deployment Guide (GitHub Clone, First-Time User)

What this does

Dispatcharr-Maid runs as Docker containers that:

• Connect to an existing Dispatcharr instance on the same Docker network
• Persist data such as CSV files, logs, and config on the host
• Expose a web monitor on port 5000
• Provide a CLI container for on-demand analysis
• Automatically restart unless manually stopped

Dispatcharr-Maid does not provide streams or content. It operates only on data already configured in Dispatcharr.

Prerequisites

Before starting, you must have:

• Docker installed
• Docker Compose available (docker compose or docker-compose)
• Dispatcharr already running in Docker
• Dispatcharr credentials

Important assumptions used by this guide:

• Dispatcharr is reachable on the Docker network by the container name dispatcharr
• Dispatcharr listens on port 9191 inside Docker

If your Dispatcharr container uses a different name or port, you must adjust the .env file accordingly.

Step 1. Clone the repository

On the machine where Docker is running:

git clone https://github.com/Biggregw/dispatcharr-maid.git
cd dispatcharr-maid

Correct behaviour

• The repository clones without errors
• You are now in the project root directory

You should see files such as:

• Dockerfile
• docker-compose.yml
• api_utils.py
• interactive_maid.py
• web_monitor.py
• config.yaml
• .env.template
• requirements.txt
• csv/ (may be empty or created later)
• logs/ (may be empty or created later)

Screenshot worth capturing

• Terminal showing the successful clone
• ls output in the repo root

Step 2. Configure Dispatcharr credentials

Copy the environment template:

cp .env.template .env


Edit the file:

nano .env


The file should look like this:

DISPATCHARR_BASE_URL=http://dispatcharr:9191
DISPATCHARR_USER=admin
DISPATCHARR_PASS=your_actual_password
DISPATCHARR_TOKEN=


Key points:

• Use the container name dispatcharr, not localhost
• Port 9191 must match your Dispatcharr container
• DISPATCHARR_TOKEN is optional and may be left empty

Save and exit nano.

Correct behaviour

• .env file exists in the repo root
• Credentials are set correctly

Screenshot worth capturing

• .env file open in nano, with secrets redacted

Step 3. Build and start the containers

From the repository root:

docker-compose build
docker-compose up -d


Check container status:

docker-compose ps


You should see output similar to:

NAME                    STATUS              PORTS
dispatcharr-maid        Up
dispatcharr-maid-web    Up (healthy)        0.0.0.0:5000->5000/tcp

Correct behaviour

• Both containers start
• dispatcharr-maid-web reports healthy
• No immediate errors in logs

If docker-compose is not available, use docker compose instead.

Screenshot worth capturing

• docker-compose ps output showing both containers running

Step 4. Access the web monitor

Open a browser and navigate to:

http://YOUR-SERVER-IP:5000


Replace YOUR-SERVER-IP with the IP or hostname of your Docker host.

Correct behaviour

• The Dispatcharr-Maid dashboard loads
• The page refreshes automatically
• No authentication prompt appears

Screenshot worth capturing

• First successful dashboard load

Step 5. Run analysis jobs
Method 1. Interactive mode (recommended)

Start an interactive session:

docker-compose exec dispatcharr-maid python3 interactive_maid.py


You should see a menu similar to:

1. Full pipeline + cleanup
2. Full pipeline
...


Follow the prompts to select groups and run analysis.

Method 2. Direct execution

You can also exec directly:

docker exec -it dispatcharr-maid python3 interactive_maid.py

Correct behaviour

• Menu appears immediately
• No authentication errors
• Group selection works

Screenshot worth capturing

• Interactive menu in the terminal

Monitoring progress

While analysis is running, open:

http://YOUR-SERVER-IP:5000


The dashboard updates automatically and shows:

• Progress bar
• Streams processed and failed
• Estimated time remaining
• Last run statistics

Viewing logs
docker-compose logs -f dispatcharr-maid-web
docker-compose logs -f dispatcharr-maid
docker-compose logs -f

Correct behaviour

• Logs stream without errors
• Activity appears during analysis runs

Accessing data files

All persistent data lives in the cloned repository directory.

ls -lh csv/
ls -lh logs/
cat config.yaml


These directories are bind-mounted into the containers.

Correct behaviour

• Files appear after analysis runs
• Files remain after container restarts

Container management
docker-compose up -d
docker-compose down
docker-compose restart
docker-compose stop dispatcharr-maid
docker-compose start dispatcharr-maid

Updating Dispatcharr-Maid

From the repo root:

git pull
docker-compose build
docker-compose up -d

Troubleshooting
Containers will not start
docker-compose logs dispatcharr-maid


Common causes:

• Missing .env file
• Incorrect Dispatcharr URL
• Network name mismatch

Cannot connect to Dispatcharr
docker exec dispatcharr-maid ping dispatcharr
docker exec dispatcharr-maid cat .env

Web monitor not accessible
docker-compose ps dispatcharr-maid-web
sudo netstat -tlnp | grep 5000
docker-compose logs dispatcharr-maid-web

Data persistence

The following host paths are mounted into containers:

Host path	Container path	Purpose
./csv/	/app/csv/	CSV output
./logs/	/app/logs/	Logs and checkpoints
./config.yaml	/app/config.yaml	Configuration
./.env	/app/.env	Credentials

Data persists across container restarts and rebuilds.

Auto-start behaviour

Containers use restart: unless-stopped:

• Restart on reboot
• Restart on crash
• Do not restart if manually stopped

End of first-time setup

At this point:

• Containers are running
• Web monitor is accessible
• Interactive analysis works
• Data persists correctly
