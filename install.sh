#!/usr/bin/env bash
set -eux

# --- Configuration ---
# Bind address for the Gunicorn server. Matches the port 5000 used in SHME_Scraping_base.py
GUNICORN_BIND="0.0.0.0:5000"
# CRITICAL: Module is set to the correct filename (SHME_Scraping_base) and Flask app instance (app)
FLASK_APP_MODULE="SHME_Scraping_base:app"
# Gunicorn settings for I/O-bound tasks (Scrapy/Playwright)
GUNICORN_WORKERS=2
GUNICORN_THREADS=4
# ---------------------

## 1. Install System Dependencies 💻

echo "INFO: Updating apt and installing necessary system dependencies..."
# Libraries required for Playwright (browser) and psycopg2 (PostgreSQL connection)
apt-get update
apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    libffi-dev \
    libnss3 \
    libxss1 \
    libasound2 \
    libxtst6 \
    libglib2.0-0 \
    libgl1-mesa-glx \
    libgtk-3-0 \
    libnotify4 \
    libcups2 \
    libxrandr2 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libatk-bridge2.0-0 \
    python3-dev \
    wget 

## 2. Install Python Packages and Playwright Browser 📦

echo "INFO: Installing Python requirements..."

# Install all required Python packages
pip install --no-cache-dir --upgrade \
    flask \
    flasgger \
    scrapy \
    scrapy-playwright \
    psycopg2-binary \
    apscheduler \
    gunicorn \
    twisted \
    parsel \
    lxml

echo "INFO: Installing Playwright Chromium browser binaries..."
# Installs the browser necessary for scrapy-playwright
python3 -m playwright install chromium --with-deps

## 3. Verification Checks ✅

echo "INFO: Verifying critical Python installations..."
python3 -c "import flask; print('✓ Flask OK')" || echo "✗ Flask FAILED"
python3 -c "import scrapy; print('✓ Scrapy OK')" || echo "✗ Scrapy FAILED"
python3 -c "import psycopg2; print('✓ Psycopg2 OK')" || echo "✗ Psycopg2 FAILED"
python3 -c "import playwright; print('✓ Playwright OK')" || echo "✗ Playwright FAILED"

## 4. Start the Gunicorn Server (Final Execution) 🚀

echo "INFO: Starting Gunicorn server on ${GUNICORN_BIND}..."
# 'exec' replaces the shell process, ensuring Gunicorn is the main running process.
exec gunicorn \
    -k gthread \
    -w ${GUNICORN_WORKERS} \
    --threads ${GUNICORN_THREADS} \
    -b ${GUNICORN_BIND} \
    --timeout 300 \
    --max-requests 1000 \
    --log-level info \
    ${FLASK_APP_MODULE}
