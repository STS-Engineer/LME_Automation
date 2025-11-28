# API Flask pour extraction des prix de métaux depuis Shmet
"""
Installation requise:
pip install flask flasgger scrapy scrapy-playwright twisted[tls] pyopenssl service_identity parsel psycopg2-binary apscheduler
playwright install chromium

VERSION STABLE CORRIGÉE POUR DÉPLOIEMENT
"""
import sys
import os
import time
from datetime import datetime
import logging
from threading import Thread, Lock, Event
from queue import Queue, Empty
import re
import json
import traceback

# ==============================
# INSTALLATION DU REACTOR ASYNCIO
# ==============================
# L'installation doit se faire avant tout import de Twisted si possible
from twisted.internet import asyncioreactor
try:
    asyncioreactor.install()
except Exception as e:
    # Le reactor pourrait être déjà installé si le processus est réutilisé (moins fréquent en prod)
    pass
    
from twisted.internet import reactor, defer

from flask import Flask, jsonify, request
from flasgger import Swagger
import psycopg2
from psycopg2.extras import RealDictCursor
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

# Import Scrapy
import scrapy
from parsel import Selector
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from scrapy_playwright.page import PageMethod

# ==============================
# CONFIGURATION DU LOGGING
# ==============================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Réduire logs verbeux
logging.getLogger('scrapy').setLevel(logging.ERROR)
logging.getLogger('filelock').setLevel(logging.ERROR)
logging.getLogger('playwright').setLevel(logging.ERROR)
logging.getLogger('apscheduler').setLevel(logging.INFO) # On garde les logs du scheduler pour le suivi

logger.info("="*80)
logger.info("🚀 DÉMARRAGE APPLICATION - VERSION STABLE CORRIGÉE")
logger.info("="*80)

# ==============================
# CONFIGURATION BASE DE DONNÉES
# ==============================
# REMARQUE : Utiliser des variables d'environnement est fortement recommandé en production
DB_CONFIG = {
    "user": "administrationSTS",
    "password": "St$@0987",
    "host": "avo-adb-002.postgres.database.azure.com",
    "port": "5432",
    "database": "LME_DB",
    "sslmode": "require"
}

METAL_MAPPING = {
    "Cu cathode 1#": "copper",
    "Zn ingot 0#, Shanghai": "zinc",
    "Tin ingot 1#(99.9%),East China": "tin"
}

URL_BASE = "https://en.shmet.com/Home"

TARGETS = [
    "Cu cathode 1#",
    "Zn ingot 0#, Shanghai",
    "Tin ingot 1#(99.9%),East China",
]

# Variables globales
reactor_ready = Event()  # Event pour signaler que le reactor est prêt
scraping_lock = Lock()
runner = None
scheduler = None # Déclarer globalement ici

logger.info(f"🎯 {len(TARGETS)} produits configurés")


# ==============================
# FONCTIONS BASE DE DONNÉES
# ==============================
def get_db_connection():
    """Créer une connexion à la base de données."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        logger.error(f"❌ Erreur connexion DB: {e}")
        raise

def save_prices_to_db(data, source_url=URL_BASE, price_datetime=None):
    """Enregistrer les prix dans la base de données."""
    conn = None
    inserted_count = 0
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        if price_datetime is None:
            price_datetime = datetime.now()
        
        price_date = price_datetime.date()
        
        for product_name, price in data.items():
            if price is None:
                continue
            
            metal_type = METAL_MAPPING.get(product_name)
            if not metal_type:
                continue
            
            insert_query = """
                INSERT INTO metal_prices 
                (source_product_name, metal_type, price, currency, unit, source_url, price_date, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(insert_query, (
                product_name, metal_type, price, 'CNY', 'ton', source_url, price_date, price_datetime
            ))
            inserted_count += 1
            # logger.info(f"    ✅ {product_name} = {price} CNY (date: {price_datetime})")
            
        conn.commit()
        logger.info(f"✅ {inserted_count} prix enregistrés avec date {price_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
        return inserted_count
        
    except Exception as e:
        logger.error(f"❌ Erreur enregistrement: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            # Assurez-vous que le curseur et la connexion sont toujours fermés
            try:
                cursor.close()
                conn.close()
            except:
                pass


def log_sync_operation(sync_type, status, metals_updated, error_message=None, duration=None):
    """Enregistrer une opération de synchronisation."""
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        insert_query = """
            INSERT INTO sync_logs 
            (sync_type, status, metals_updated, error_message, duration_seconds, created_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
        """
        
        cursor.execute(insert_query, (sync_type, status, metals_updated, error_message, duration))
        conn.commit()
        logger.info(f"📝 Log: {status} - {metals_updated} métaux")
        
    except Exception as e:
        logger.error(f"❌ Erreur log: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            try:
                cursor.close()
                conn.close()
            except:
                pass


# ==============================
# EXTRACTION
# ==============================
def extract_from_dom(response: scrapy.http.Response):
    """Extraction depuis le DOM rendu."""
    sel = Selector(text=response.text)
    data = {target: None for target in TARGETS}
    
    sections = sel.xpath('//div[contains(@class, "card-title") and contains(@class, "pull-left")]')
    
    china_section = None
    for section in sections:
        title = section.xpath('string(.)').get("").strip()
        if "China Domestic Market Price" in title:
            china_section = section
            break
    
    if not china_section:
        logger.warning("⚠️  Section 'China Domestic Market Price' non trouvée")
        return data
    
    # Naviguer jusqu'au parent 'card' ou 'panel'
    parent_card = china_section.xpath('ancestor::div[contains(@class, "card") or contains(@class, "panel")]')
    if not parent_card:
        logger.warning("⚠️  Parent card non trouvé pour la section")
        return data
        
    rows = parent_card[0].css("tr.el-table__row")
    # logger.info(f"    📊 {len(rows)} lignes trouvées")
    
    for row in rows:
        name_el = row.css("td span.cell-name")
        # Tentative d'extraction de la 3ème colonne pour la valeur (la colonne "price" dans le tableau)
        val_el = row.css("td.el-table_1_column_3 div.cell")
        
        if not name_el or not val_el:
            # Fallback générique si la structure change légèrement
            val_el = row.css("td:nth-child(3) div.cell")
        
        if not name_el or not val_el:
            continue
        
        name = name_el.xpath("string(.)").get("").strip()
        raw_value = val_el.xpath("string(.)").get("").strip()
        
        clean_value = raw_value.replace(",", "")
        # Regex pour ne garder que le nombre (y compris le signe moins et le point)
        numeric_value = re.sub(r"[^\d\.\-]", "", clean_value)
        
        if not numeric_value:
            continue
        
        try:
            price = float(numeric_value)
            
            for target in TARGETS:
                if target.lower().strip() == name.lower().strip(): # Match exact ou proche
                    data[target] = price
                    # logger.info(f"    ✅ {target} = {price}")
                    break
        except ValueError:
            continue
    
    found = sum(1 for v in data.values() if v is not None)
    logger.info(f"✅ {found}/{len(TARGETS)} extraits du DOM")
    return data


# ==============================
# SPIDER SCRAPY
# ==============================
class ShmetSpider(scrapy.Spider):
    name = "shmet_spider"
    
    # Settings pour l'environnement de production
    custom_settings = {
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
        "PLAYWRIGHT_BROWSER_TYPE": "chromium",
        "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": 60000,
        "PLAYWRIGHT_LAUNCH_OPTIONS": {
            # Ces arguments sont CRUCIAUX pour l'exécution sans GUI sur Azure
            "headless": True,
            "args": ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage", "--single-process"] 
        },
        "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "ROBOTSTXT_OBEY": False,
        "LOG_ENABLED": False,
        "CONCURRENT_REQUESTS": 1,
        "DOWNLOAD_DELAY": 1, # Réduit le délai pour accélérer l'exécution
        "DOWNLOAD_TIMEOUT": 60,
    }
    
    def __init__(self, result_callback=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.result_callback = result_callback
    
    def start_requests(self):
        logger.info(f"🌐 Requête Playwright planifiée pour: {URL_BASE}")
        yield scrapy.Request(
            url=URL_BASE,
            meta={
                "playwright": True,
                "playwright_page_methods": [
                    # Attendre que les lignes de tableau soient rendues
                    PageMethod("wait_for_selector", "tr.el-table__row", timeout=30000), 
                    # Délai supplémentaire pour le chargement des données (souvent nécessaire pour JS/Vue/React)
                    PageMethod("wait_for_timeout", 3000), 
                ],
                "playwright_include_page": False,
            },
            callback=self.parse,
            errback=self.errback,
            dont_filter=True,
        )
    
    def parse(self, response):
        """Parse la réponse."""
        # logger.info(f"📄 Parsing {len(response.text)} chars")
        
        data = extract_from_dom(response)
        
        result = {
            "data": data,
            "url": response.url,
            "timestamp": datetime.now().isoformat(),
        }
        
        if self.result_callback:
            self.result_callback(result)
        
        # Le yield est plus pour le pipeline Scrapy standard, on s'appuie sur le callback
        # yield result 
    
    def errback(self, failure):
        """Gestion erreurs."""
        error_message = f"Erreur Scrapy/Playwright: {failure.value}"
        logger.error(f"❌ {error_message}")
        result = {
            "data": {target: None for target in TARGETS},
            "url": URL_BASE,
            "timestamp": datetime.now().isoformat(),
            "error": error_message
        }
        
        if self.result_callback:
            self.result_callback(result)
        
        # yield result


# ==============================
# GESTION REACTOR
# ==============================
def start_reactor():
    """Démarrer le reactor Twisted."""
    logger.info("🔄 Démarrage reactor Twisted...")
    try:
        reactor_ready.set()  # Signaler que le reactor est prêt
        # L'installationSignalHandlers=False est crucial pour ne pas interférer avec le processus parent (Gunicorn)
        reactor.run(installSignalHandlers=False) 
    except Exception as e:
        logger.error(f"❌ Erreur critique lors du démarrage du reactor: {e}")
    logger.info("🛑 Reactor arrêté")


def wait_for_reactor(timeout=10):
    """Attendre que le reactor soit prêt."""
    if not reactor_ready.wait(timeout=timeout):
        logger.error("❌ Timeout: reactor non prêt")
        raise TimeoutError("Reactor non initialisé après le délai imparti")
    logger.info("✅ Reactor prêt")


# ==============================
# FONCTION DE SCRAPING
# ==============================
def scrape_and_save(sync_type='manual', scheduled_datetime=None):
    """Effectue le scraping et enregistre."""
    start_time = time.time()
    
    # 1. Vérifier le lock (si un autre scraping est déjà en cours)
    if not scraping_lock.acquire(blocking=False):
        logger.warning("⚠️  Scraping déjà en cours (lock actif)")
        return {
            "status": "warning",
            "message": "Scraping déjà en cours",
            "sync_type": sync_type
        }
    
    try:
        # Capturer la date/heure de début si non fournie (pour l'enregistrement DB)
        scraping_datetime = scheduled_datetime if scheduled_datetime is not None else datetime.now()
        
        logger.info("="*80)
        logger.info(f"🚀 EXTRACTION ({sync_type}) - {scraping_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("="*80)
        
        # 2. Attendre que le reactor soit prêt (jusqu'à 30s)
        try:
            wait_for_reactor(timeout=30)
        except TimeoutError as e:
            logger.error(f"❌ {e}")
            log_sync_operation(sync_type, 'failed', 0, str(e), time.time() - start_time)
            return {"status": "error", "message": str(e), "sync_type": sync_type}
        
        # 3. Préparer le résultat
        result_data = {"completed": False, "result": None, "lock": Lock()}
        
        def result_callback(result):
            """Callback quand le spider termine."""
            with result_data["lock"]:
                result_data["result"] = result
                result_data["completed"] = True
        
        # 4. Fonction de crawl Twisted
        @defer.inlineCallbacks
        def crawl():
            try:
                global runner
                if runner is None:
                    # Configuration du runner Scrapy
                    configure_logging({'LOG_ENABLED': False})
                    runner = CrawlerRunner(ShmetSpider.custom_settings)
                
                # Lancer le spider
                d = runner.crawl(ShmetSpider, result_callback=result_callback)
                # Attendre la fin du crawl
                yield d
                logger.info("✅ Crawl terminé (Twisted)")
            except Exception as e:
                logger.error(f"❌ Erreur crawl (Twisted): {e}")
                # Assurez-vous d'appeler le callback même en cas d'erreur dans le crawl
                result_callback({
                    "data": {target: None for target in TARGETS},
                    "url": URL_BASE,
                    "timestamp": datetime.now().isoformat(),
                    "error": f"Erreur interne Twisted: {str(e)}"
                })

        # 5. Planifier le crawl dans le reactor
        # Ceci est CRUCIAL: le crawl doit être appelé depuis un thread du reactor
        reactor.callFromThread(crawl) 
        logger.info("📤 Crawl planifié (dans le reactor thread pool)")
        
        # 6. Attendre le résultat (blocage du thread actuel)
        timeout = 180  # 3 minutes
        elapsed = 0
        check_interval = 2
        
        while elapsed < timeout:
            time.sleep(check_interval)
            elapsed += check_interval
            
            with result_data["lock"]:
                if result_data["completed"]:
                    break
            
            if elapsed % 20 == 0:
                logger.info(f"⏳ Attente {elapsed}s...")
        
        with result_data["lock"]:
            if not result_data["completed"]:
                duration = time.time() - start_time
                logger.error(f"⏱️  TIMEOUT après {duration:.2f}s")
                log_sync_operation(sync_type, 'failed', 0, 'Timeout', duration)
                return {
                    "status": "error",
                    "message": f"Timeout après {duration:.2f}s",
                    "sync_type": sync_type
                }
            
            result = result_data["result"]
        
        # 7. Traiter et Sauvegarder
        if "error" in result:
            duration = time.time() - start_time
            error_msg = result.get("error")
            logger.error(f"❌ Erreur: {error_msg}")
            log_sync_operation(sync_type, 'failed', 0, error_msg, duration)
            return {"status": "error", "message": error_msg, "sync_type": sync_type}
        
        data = result.get("data", {})
        metals_updated = save_prices_to_db(data, result.get("url"), scraping_datetime)
        
        duration = time.time() - start_time
        
        status = 'success' if metals_updated == len(TARGETS) else ('partial' if metals_updated > 0 else 'failed')
        
        log_sync_operation(sync_type, status, metals_updated, None, duration)
        
        logger.info("="*80)
        logger.info(f"✅ TERMINÉ ({duration:.2f}s) - {metals_updated}/{len(TARGETS)}")
        logger.info(f"    📅 Date enregistrement: {scraping_datetime}")
        logger.info("="*80)
        
        return {
            "status": status,
            "data": data,
            "metals_saved": metals_updated,
            "total_targets": len(TARGETS),
            "duration": duration,
            "sync_type": sync_type,
            "timestamp": result.get("timestamp")
        }
        
    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"❌ Erreur générale: {e}")
        logger.error(traceback.format_exc())
        
        log_sync_operation(sync_type, 'failed', 0, str(e), duration)
        
        return {"status": "error", "message": str(e), "sync_type": sync_type}
        
    finally:
        scraping_lock.release()


# ==============================
# TÂCHE PLANIFIÉE
# ==============================
def scheduled_scraping_job():
    """Tâche planifiée - lance la fonction de scraping dans un nouveau thread."""
    scheduled_time = datetime.now()
    
    logger.info("="*80)
    logger.info(f"⏰ TÂCHE PLANIFIÉE DÉCLENCHÉE - {scheduled_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*80)
    
    # Lancement dans un thread pour éviter de bloquer le pool de threads de l'APScheduler
    def run_scraping_in_thread():
        try:
            result = scrape_and_save(sync_type='scheduled', scheduled_datetime=scheduled_time)
            logger.info(f"📊 Résultat scheduled: {result.get('status')} - Métaux sauvés: {result.get('metals_saved')}")
        except Exception as e:
            logger.error(f"❌ Erreur critique dans thread planifié: {e}")
            
    thread = Thread(target=run_scraping_in_thread, daemon=True, name="ScheduledScrapingThread")
    thread.start()


# ==============================
# APPLICATION FLASK
# ==============================
app = Flask(__name__)

# Déterminer le HOST pour Swagger (CRUCIAL pour le déploiement)
# On utilise la variable d'environnement ou le lien fourni par l'utilisateur
DEPLOYED_HOST = os.environ.get('WEBSITE_HOSTNAME', 'api-exmetal.azurewebsites.net') 

# Configuration et initialisation de Swagger
swagger_config = {
    "headers": [],
    "specs": [{
        "endpoint": "apispec",
        "route": "/apispec.json",
        "rule_filter": lambda rule: True,
        "model_filter": lambda tag: True,
    }],
    "static_url_path": "/flasgger_static",
    "swagger_ui": True,
    "specs_route": "/docs"
}

swagger_template = {
    "swagger": "2.0",
    "info": {
        "title": "API Extraction Prix Métaux",
        "description": "API pour extraire et gérer les prix des métaux depuis Shmet",
        "version": "3.1-CORRECTED",
        "contact": {"name": "Support API"},
    },
    # MISE À JOUR CRITIQUE: Utiliser le host déployé pour que Swagger UI fonctionne
    "host": DEPLOYED_HOST, 
    "basePath": "/",
    "schemes": ["http", "https"],
}

swagger = Swagger(app, config=swagger_config, template=swagger_template)

# ==============================
# ROUTES API
# ==============================
@app.route("/", methods=["GET"])
def home():
    """
    Accueil de l'API
    ---
    responses:
      200:
        description: Informations sur le service et les endpoints disponibles
        schema:
          type: object
          properties:
            service: {type: string}
            version: {type: string}
            endpoints: {type: object}
    """
    return jsonify({
        "service": "API extraction prix métaux",
        "version": "3.1-CORRECTED",
        "endpoints": {
            "/extract": "POST - Extraction manuelle",
            "/prices/latest": "GET - Derniers prix",
            "/prices/history": "GET - Historique",
            "/sync/logs": "GET - Logs",
            "/health": "GET - Santé",
            "/targets": "GET - Produits",
            "/docs": "GET - Documentation (Swagger UI)"
        }
    })

@app.route("/health", methods=["GET"])
def health_check():
    """
    Vérification de la santé du service
    Vérifie la connexion à la base de données, l'état du reactor Twisted et du scheduler APScheduler.
    ---
    tags:
      - Monitoring
    responses:
      200:
        description: Statut du service
        schema:
          type: object
          properties:
            status: {type: string, description: "healthy ou unhealthy"}
            database: {type: string, description: "connected ou disconnected"}
            reactor: {type: string, description: "running ou starting"}
            scheduler: {type: string, description: "running ou stopped"}
            timestamp: {type: string, format: date-time}
    """
    db_status = "unknown"
    try:
        conn = get_db_connection()
        conn.close()
        db_status = "connected"
    except:
        db_status = "disconnected"
    
    return jsonify({
        "status": "healthy",
        "database": db_status,
        "reactor": "running" if reactor_ready.is_set() else "starting",
        "scheduler": "running" if scheduler and scheduler.running else "stopped",
        "timestamp": datetime.now().isoformat()
    })

@app.route("/targets", methods=["GET"])
def get_targets():
    """
    Liste des produits et leur mapping interne
    ---
    tags:
      - Configuration
    responses:
      200:
        description: Liste des produits ciblés pour l'extraction
        schema:
          type: object
          properties:
            targets: {type: array, items: {type: string}}
            count: {type: integer}
            mapping: {type: object, description: "Mapping du nom du produit à un type de métal générique"}
    """
    return jsonify({
        "targets": TARGETS,
        "count": len(TARGETS),
        "mapping": METAL_MAPPING
    })

@app.route("/extract", methods=["POST"])
def extract_prices():
    """
    Déclenchement manuel de l'extraction
    Lance la procédure de scraping des prix depuis Shmet et les enregistre en base de données.
    ---
    tags:
      - Extraction
    responses:
      200:
        description: Extraction lancée avec succès ou déjà en cours (warning)
        schema:
          type: object
          properties:
            status: {type: string, description: "success, partial, ou warning (si déjà en cours)"}
            data: {type: object, description: "Prix extraits (clés = noms des produits)"}
            metals_saved: {type: integer}
            total_targets: {type: integer}
            duration: {type: number}
            sync_type: {type: string}
            timestamp: {type: string, format: date-time}
      500:
        description: Erreur interne, timeout ou échec de la connexion/scraping
        schema:
          type: object
          properties:
            status: {type: string, description: "error"}
            message: {type: string}
            sync_type: {type: string}
    """
    logger.info("🎯 /extract (manuel)")
    # Lancement du scraping
    result = scrape_and_save(sync_type='manual')
    
    if result["status"] in ["success", "partial", "warning"]:
        return jsonify(result), 200
    else:
        # En cas d'erreur ou de timeout
        return jsonify(result), 500

@app.route("/prices/latest", methods=["GET"])
def get_latest_prices():
    """
    Récupération des derniers prix
    Récupère le prix le plus récent pour chaque type de métal, ou l'historique récent d'un type spécifique.
    ---
    tags:
      - Prix
    parameters:
      - name: metal_type
        in: query
        type: string
        enum: [copper, zinc, tin]
        description: Filtre optionnel sur le type de métal (si absent, retourne les derniers de tous les types)
    responses:
      200:
        description: Derniers prix enregistrés
        schema:
          type: object
          properties:
            status: {type: string}
            count: {type: integer}
            prices:
              type: array
              items:
                type: object
                properties:
                  metal_type: {type: string}
                  price: {type: number}
                  created_at: {type: string}
      500:
        description: Erreur de base de données
    """
    metal_type = request.args.get("metal_type")
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        if metal_type:
            # Récupère les 10 dernières entrées pour le type de métal spécifié
            query = "SELECT * FROM metal_prices WHERE metal_type = %s ORDER BY created_at DESC LIMIT 10"
            cursor.execute(query, (metal_type,))
        else:
            # Récupère la dernière entrée pour chaque type de métal (en utilisant DISTINCT ON)
            query = "SELECT DISTINCT ON (metal_type) * FROM metal_prices ORDER BY metal_type, created_at DESC"
            cursor.execute(query)
        
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return jsonify({"status": "success", "count": len(results), "prices": results}), 200
        
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/prices/history", methods=["GET"])
def get_price_history():
    """
    Récupération de l'historique des prix
    Récupère les prix sur une période ou avec une limite spécifique.
    ---
    tags:
      - Prix
    parameters:
      - name: metal_type
        in: query
        type: string
        enum: [copper, zinc, tin]
        description: Type de métal à filtrer
      - name: days
        in: query
        type: integer
        default: 7
        description: Nombre de jours d'historique à inclure
      - name: limit
        in: query
        type: integer
        default: 100
        description: Nombre maximum d'enregistrements à retourner
    responses:
      200:
        description: Historique des prix filtré
        schema:
          type: object
          properties:
            status: {type: string}
            count: {type: integer}
            filters: {type: object}
            history:
              type: array
              items:
                type: object
    500:
      description: Erreur de base de données
    """
    metal_type = request.args.get("metal_type")
    days = request.args.get("days", default=7, type=int)
    limit = request.args.get("limit", default=100, type=int)
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        if metal_type:
            query = "SELECT * FROM metal_prices WHERE metal_type = %s AND created_at >= NOW() - INTERVAL '%s days' ORDER BY created_at DESC LIMIT %s"
            cursor.execute(query, (metal_type, days, limit))
        else:
            query = "SELECT * FROM metal_prices WHERE created_at >= NOW() - INTERVAL '%s days' ORDER BY created_at DESC LIMIT %s"
            cursor.execute(query, (days, limit))
        
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return jsonify({
            "status": "success",
            "count": len(results),
            "filters": {"metal_type": metal_type, "days": days, "limit": limit},
            "history": results
        }), 200
        
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/sync/logs", methods=["GET"])
def get_sync_logs():
    """
    Récupération des logs de synchronisation
    Affiche l'historique des tentatives de scraping (manuel ou planifié).
    ---
    tags:
      - Monitoring
    parameters:
      - name: limit
        in: query
        type: integer
        default: 50
        description: Nombre maximum de logs à retourner
    responses:
      200:
        description: Liste des logs de synchronisation
        schema:
          type: object
          properties:
            status: {type: string}
            count: {type: integer}
            logs:
              type: array
              items:
                type: object
    500:
      description: Erreur de base de données
    """
    limit = request.args.get("limit", default=50, type=int)
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        query = "SELECT * FROM sync_logs ORDER BY created_at DESC LIMIT %s"
        cursor.execute(query, (limit,))
        
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return jsonify({"status": "success", "count": len(results), "logs": results}), 200
        
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ==============================
# INITIALISATION
# ==============================
def initialize_app():
    """Initialiser le reactor et le scheduler dans le bon ordre."""
    global scheduler
    
    logger.info("🔧 Initialisation...")
    
    # 1. Démarrer le reactor dans un thread
    # Le run() du reactor est bloquant, il DOIT être dans un thread séparé
    reactor_thread = Thread(target=start_reactor, daemon=True, name="ReactorThread")
    reactor_thread.start()
    logger.info("✅ Thread reactor lancé")
    
    # 2. Attendre que le reactor soit prêt
    try:
        wait_for_reactor(timeout=30)
    except TimeoutError:
        logger.error("❌ Le reactor n'a pas pu démarrer à temps. Le scraping sera inutilisable.")
        return False # Échec de l'initialisation
        
    # 3. Démarrer le scheduler APRÈS le reactor
    if scheduler is None:
        scheduler = BackgroundScheduler()
        # Déclenchement à 9h10 (heure du serveur où il est déployé)
        scheduler.add_job(
            func=scheduled_scraping_job,
            trigger=CronTrigger(hour=9, minute=10), 
            id='daily_scraping',
            name='Extraction quotidienne 9h10',
            replace_existing=True
        )
        scheduler.start()
        logger.info("⏰ Scheduler démarré: extraction planifiée à 9h10 (heure du serveur)")
    
    logger.info("✅ Initialisation terminée")
    return True


# ==============================
# POINT D'ENTRÉE (Critique pour le déploiement)
# ==============================

# L'initialisation doit être appelée lorsque le serveur WSGI (Gunicorn) démarre.
# Pour le développement local (pour le test), on garde le bloc __main__.

if __name__ == "__main__":
    logger.info("="*80)
    logger.info("🚀 DÉMARRAGE SERVEUR LOCAL DE DÉVELOPPEMENT (via app.run)")
    logger.info("="*80)
    
    if initialize_app():
        logger.info(f"📊 Documentation: http://{DEPLOYED_HOST}/docs (ou http://localhost:5000/docs en local)")
        logger.info(f"🎯 {len(TARGETS)} produits")
        logger.info("="*80)
        
        try:
            # use_reloader=False est important pour éviter de démarrer le reactor deux fois en mode dev
            # threaded=True est requis car le scraping bloque le thread
            app.run(host="0.0.0.0", port=5000, debug=False, threaded=True, use_reloader=False)
        except (KeyboardInterrupt, SystemExit):
            if scheduler:
                scheduler.shutdown()
            logger.info("🛑 Arrêt du serveur")
        except Exception as e:
            logger.error(f"❌ Erreur fatale du serveur: {e}")
            raise
    else:
        logger.error("❌ Arrêt car l'initialisation du Reactor a échoué.")
