# Dockerfile (Optimized Version with Pre-installed Chromedriver)

FROM python:3.11-slim-bookworm
ENV PYTHONUNBUFFERED True
ENV DEBIAN_FRONTEND=noninteractive

# --- OPTIMIZATION: Install system dependencies and Chrome ---
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    gnupg \
    ca-certificates \
    unzip \
    && wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list' \
    && apt-get update && apt-get install -y google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

# --- OPTIMIZATION: Download and install the matching chromedriver ---
RUN CHROME_VERSION=$(google-chrome --version | cut -f 3 -d ' ' | cut -d '.' -f 1-3) && \
    CHROMEDRIVER_VERSION=$(wget -qO- "https://googlechromelabs.github.io/chrome-for-testing/known-good-versions-with-downloads.json" | jq -r ".versions[] | select(.version | startswith(\"${CHROME_VERSION}\")) | .downloads.chromedriver[0].url") && \
    wget -q --continue -P /tmp/ "${CHROMEDRIVER_VERSION}" && \
    unzip /tmp/chromedriver-linux64.zip -d /usr/bin/ && \
    rm /tmp/chromedriver-linux64.zip

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "1", "--threads", "8", "--timeout", "0", "main:app"]