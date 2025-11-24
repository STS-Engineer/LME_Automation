#!/bin/bash

# 1. Installe les dépendances Python, forçant la mise à jour (upgrade) si nécessaire
# L'option --upgrade garantit que les versions de Flask et Flasgger sont bien celles demandées.
echo "Installing/Upgrading Python requirements..."
pip install --upgrade --force-reinstall -r requirements.txt

# 2. Installe le navigateur Playwright (nécessaire pour scrapy-playwright)
echo "Installing Playwright Chromium browser..."
python -m playwright install chromium --with-deps

echo "Installation complete."
