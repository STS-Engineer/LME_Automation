#!/bin/bash

PYTHON_EXEC="python3" # Tentez python3 comme alias plus générique

echo "========================================================"
echo "-> 1/3. Installation/Mise à jour des dépendances Python (incluant flasgger)..."

# --- MODIFICATION CLÉ CI-DESSOUS ---
# 1. Installe les dépendances listées dans requirements.txt
# 2. Installe ENSUITE les dépendances TLS requises par Twisted pour le support HTTPS
pip install --upgrade -r requirements.txt
pip install pyopenssl service_identity twisted[tls]
# ------------------------------------

if [ $? -ne 0 ]; then
    echo "❌ ERREUR FATALE: L'installation des dépendances Python (pip) a échoué. Cause probable: problème de librairie système."
    exit 1
fi

echo "-> 2/3. Installation du navigateur Playwright Chromium..."
# Utilisez l'alias python3 pour cette commande aussi
$PYTHON_EXEC -m playwright install chromium --with-deps
if [ $? -ne 0 ]; then
    echo "❌ ERREUR FATALE: L'installation de Playwright Chromium a échoué."
    exit 1
fi

echo "--------------------------------------------------------"
echo "✅ INSTALLATION TERMINÉE. Lancement de Gunicorn..."

# 3. Démarrage de l'application avec Gunicorn
echo "-> 3/3. Lancement de Gunicorn: SHME_Scraping_base:app sur le port 8000..."
# L'erreur de libpython pourrait toujours affecter le lancement de Gunicorn, mais nous tentons
gunicorn --bind 0.0.0.0 --timeout 300 SHME_Scraping_base:app --preload
