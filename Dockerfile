# Dockerfile (New Version with Google Chrome Installed)

# Use an official Python image that is based on Debian
FROM python:3.11-slim-bookworm

# Set environment variables for best practices
ENV PYTHONUNBUFFERED True
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies for Chrome, including fonts and libraries
# This is the most important new section
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    gnupg \
    ca-certificates \
    # Add Google Chrome's official repository
    && wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list' \
    # Install Chrome
    && apt-get update && apt-get install -y google-chrome-stable \
    # Clean up to keep the container size small
    && rm -rf /var/lib/apt/lists/*

# Set the working directory inside the container
WORKDIR /app

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your application code
COPY . .

# Command to run the application using Gunicorn
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "1", "--threads", "8", "--timeout", "0", "main:app"]