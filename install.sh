#!/bin/bash

# 1. Installation des dépendances
echo "Forcing installation of exact dependency versions..."
pip install --upgrade --force-reinstall -r requirements.txt

# 2. Installation du navigateur Playwright
echo "Installing Playwright Chromium browser dependencies..."
python -m playwright install chromium --with-deps

echo "Installation complete. Starting application..."

# 3. Démarrage de l'application (Ajouter cette ligne)
gunicorn --bind 0.0.0.0:8000 SHME_Scraping_base:app
