FROM python:3.11-slim

# Install ffmpeg and other dependencies
RUN apt-get update && apt-get install -y \
    ffmpeg \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


# Copy application files
COPY . .

# Create directories for data
RUN mkdir -p /app/csv /app/logs

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Default command (can be overridden)
# The docker-compose.yml runs the web UI, so make that the default too.
CMD ["python3", "dispatcharr_web_app.py"]
