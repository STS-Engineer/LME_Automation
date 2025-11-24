#!/bin/bash

# --- 1. Install System Dependencies (optional, but sometimes necessary for Playwright) ---
# Depending on your base OS (e.g., Debian/Ubuntu), you might need these first
# Example: 
# apt-get update && apt-get install -y libnss3 libatk-bridge2.0-0 libdrm-dev libgbm-dev libasound2

# --- 2. Install Python Packages ---
echo "Installing Python requirements from requirements.txt..."
pip install -r requirements.txt

# --- 3. Install Playwright Browser Binaries ---
echo "Installing Playwright Chromium browser..."
# Use python -m playwright instead of just 'playwright' in case the alias isn't set
python -m playwright install chromium

echo "Installation complete."