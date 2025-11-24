#!/bin/bash

echo "Forcing installation of exact dependency versions..."
pip install --upgrade --force-reinstall -r requirements.txt # <-- NOUVEAU: --force-reinstall

echo "Installing Playwright Chromium browser dependencies..."
python -m playwright install chromium --with-deps

echo "Installation complete."
