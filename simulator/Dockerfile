FROM public.ecr.aws/docker/library/python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip setuptools wheel
RUN pip install --no-cache-dir -r requirements.txt

# Copy the simulator source code
COPY src/ ./src/

# Copy the config module (loader only, config.json is mounted as volume)
COPY config/ ./config/

# Set Python path to include the src and config directories
ENV PYTHONPATH="/app/src:/app"

# Default command (can be overridden in docker-compose)
CMD ["python", "src/cli.py", "--help"]