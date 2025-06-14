# Dockerfile
# Use an official lightweight Python image
FROM python:3.11-slim

# Set environment variables for best practices in containers
ENV PYTHONUNBUFFERED True

# Set the working directory inside the container
WORKDIR /app

# Copy and install dependencies first to leverage Docker layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your application code into the container
COPY . .

# Command to run the application using Gunicorn
# It listens on port 8080, which is the default port Cloud Run expects
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "1", "--threads", "8", "--timeout", "0", "main:app"]