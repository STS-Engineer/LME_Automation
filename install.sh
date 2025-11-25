#!/bin/bash

# --- SECTION 0: System Dependencies (Must be run as root/sudo, often in Dockerfile) ---
# NOTE: This section is for installing OS-level libraries needed to compile psycopg2.
# It is typically run *before* the script, or in a Dockerfile's RUN command.
# If your environment already has libpq-dev, skip this block.

# echo "Installing system dependencies for psycopg2 (libpq-dev)..."
# sudo apt-get update
# sudo apt-get install -y libpq-dev python3-dev

# -----------------------------------------------------------------------------------

# 1. Force Uninstall/Reinstall of psycopg2
# This targets the 'undefined symbol' error by ensuring a fresh build against the
# currently active Python interpreter.
echo "Cleaning and forcing reinstallation of psycopg2-binary..."

# Use 'pip uninstall' multiple times to catch both 'psycopg2' and 'psycopg2-binary'
pip uninstall -y psycopg2-binary psycopg2

# Reinstall the binary version (easiest way to avoid compile errors if system dependencies are missing)
pip install psycopg2-binary

# -----------------------------------------------------------------------------------

# 2. Installation des autres dépendances (with force-reinstall)
echo "Forcing installation of all other exact dependency versions..."
# Use --no-deps to skip psycopg2 if it's already in requirements.txt, 
# but installing everything with --force-reinstall is generally safer if the list is consistent.
pip install --upgrade --force-reinstall -r requirements.txt

# 3. Installation du navigateur Playwright
echo "Installing Playwright Chromium browser dependencies..."
python -m playwright install chromium --with-deps

echo "Installation complete. Starting application..."

# 4. Démarrage de l'application
gunicorn --bind 0.0.0.0:8000 SHME_Scraping_base:app
