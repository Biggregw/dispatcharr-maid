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
COPY api_utils.py .
COPY stream_analysis.py .
COPY interactive_maid.py .
COPY web_monitor.py .
COPY dispatcharr_web_app.py .
COPY templates ./templates/
COPY config.yaml .

# Create directories for data
RUN mkdir -p /app/csv /app/logs

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Default command (can be overridden)
CMD ["python3", "interactive_maid.py"]
