#!/bin/bash

# --- ⚠️ IMPORTANT SYSTEM DEPENDENCY CHECK ⚠️ ---
# The "undefined symbol" error is typically a C-library linkage issue.
# This script assumes you are running on a Linux-based environment (like Azure App Service Linux).
# Ensure system libraries required for psycopg2 (like libpq-dev) are installed 
# in the underlying OS environment (e.g., in your Dockerfile or provisioning script).
# Example for Debian/Ubuntu: sudo apt-get update && sudo apt-get install -y libpq-dev python3-dev
# ------------------------------------------------

# 1. Force Uninstall/Reinstall of psycopg2-binary
# This ensures any previous broken installation is removed, and a fresh binary 
# is installed before the main requirements batch run.
echo "Cleaning and forcing reinstallation of psycopg2-binary to resolve potential C-library issues..."
pip uninstall -y psycopg2-binary psycopg2
pip install psycopg2-binary

# 2. Installation des autres dépendances Python
# Using --upgrade and --force-reinstall ensures all other packages from 
# requirements.txt are installed cleanly, including the correct versions 
# of scrapy, twisted, and playwright dependencies.
echo "Forcing clean installation of all other exact dependency versions from requirements.txt..."
pip install --upgrade --force-reinstall -r requirements.txt

# 3. Installation du navigateur Playwright
echo "Installing Playwright Chromium browser dependencies..."
python -m playwright install chromium --with-deps

echo "Installation complete. Starting application..."

# 4. Démarrage de l'application
# Added --log-level debug and --error-logfile - to force Gunicorn to output 
# detailed startup errors (like ImportError traceback) to the console/Azure Log Stream.
gunicorn --bind 0.0.0.0:8000 SHME_Scraping_base:app --log-level debug --error-logfile -
