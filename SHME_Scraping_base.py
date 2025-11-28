# API Flask pour extraction des prix de métaux depuis Shmet
"""
Installation requise:
pip install flask flasgger scrapy scrapy-playwright twisted[tls] pyopenssl service_identity parsel psycopg2-binary apscheduler
playwright install chromium

VERSION AVEC SCHEDULER STABLE
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

# ==============================
# INSTALLATION DU REACTOR ASYNCIO
# ==============================
from twisted.internet import asyncioreactor
asyncioreactor.install()

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
from twisted.internet import reactor, defer

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

logger.info("="*80)
logger.info("🚀 DÉMARRAGE APPLICATION - VERSION SCHEDULER STABLE")
logger.info("="*80)

# ==============================
# CONFIGURATION BASE DE DONNÉES
# ==============================
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
        
        # Utiliser la date/heure fournie ou celle actuelle
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
            logger.info(f"   ✅ {product_name} = {price} CNY (date: {price_datetime})")
        
        conn.commit()
        logger.info(f"✅ {inserted_count} prix enregistrés avec date {price_datetime}")
        return inserted_count
        
    except Exception as e:
        logger.error(f"❌ Erreur enregistrement: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            cursor.close()
            conn.close()


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
            cursor.close()
            conn.close()


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
        logger.warning("⚠️  Section non trouvée")
        return data
    
    parent_card = china_section.xpath('ancestor::div[contains(@class, "card") or contains(@class, "panel")]')[0]
    rows = parent_card.css("tr.el-table__row")
    logger.info(f"   📊 {len(rows)} lignes")
    
    for row in rows:
        name_el = row.css("td span.cell-name")
        val_el = row.css("td.el-table_1_column_3 div.cell")
        
        if not name_el or not val_el:
            val_el = row.css("td:nth-child(3) div.cell")
        
        if not name_el or not val_el:
            continue
        
        name = name_el.xpath("string(.)").get("").strip()
        raw_value = val_el.xpath("string(.)").get("").strip()
        
        clean_value = raw_value.replace(",", "")
        numeric_value = re.sub(r"[^\d\.\-]", "", clean_value)
        
        if not numeric_value:
            continue
        
        try:
            price = float(numeric_value)
            
            for target in TARGETS:
                if target.lower().strip() in name.lower().strip():
                    data[target] = price
                    logger.info(f"   ✅ {target} = {price}")
                    break
        except ValueError:
            continue
    
    found = sum(1 for v in data.values() if v is not None)
    logger.info(f"✅ {found}/{len(TARGETS)} extraits")
    return data


# ==============================
# SPIDER SCRAPY
# ==============================
class ShmetSpider(scrapy.Spider):
    name = "shmet_spider"
    
    custom_settings = {
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
        "PLAYWRIGHT_BROWSER_TYPE": "chromium",
        "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": 60000,
        "PLAYWRIGHT_LAUNCH_OPTIONS": {
            "headless": True,
            "args": ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"]
        },
        "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "ROBOTSTXT_OBEY": False,
        "LOG_ENABLED": False,
        "CONCURRENT_REQUESTS": 1,
        "DOWNLOAD_DELAY": 2,
        "DOWNLOAD_TIMEOUT": 60,
    }
    
    def __init__(self, result_callback=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.result_callback = result_callback
    
    def start_requests(self):
        logger.info(f"🌐 Requête: {URL_BASE}")
        yield scrapy.Request(
            url=URL_BASE,
            meta={
                "playwright": True,
                "playwright_page_methods": [
                    PageMethod("wait_for_selector", "tr.el-table__row", timeout=30000),
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
        logger.info(f"📄 Parsing {len(response.text)} chars")
        
        data = extract_from_dom(response)
        
        result = {
            "data": data,
            "url": response.url,
            "timestamp": datetime.now().isoformat(),
        }
        
        if self.result_callback:
            self.result_callback(result)
        
        yield result
    
    def errback(self, failure):
        """Gestion erreurs."""
        logger.error(f"❌ Erreur spider: {failure.value}")
        result = {
            "data": {target: None for target in TARGETS},
            "url": URL_BASE,
            "timestamp": datetime.now().isoformat(),
            "error": str(failure.value)
        }
        
        if self.result_callback:
            self.result_callback(result)
        
        yield result


# ==============================
# GESTION REACTOR
# ==============================
def start_reactor():
    """Démarrer le reactor Twisted."""
    logger.info("🔄 Démarrage reactor Twisted...")
    reactor_ready.set()  # Signaler que le reactor est prêt
    reactor.run(installSignalHandlers=False)
    logger.info("🛑 Reactor arrêté")


def wait_for_reactor(timeout=10):
    """Attendre que le reactor soit prêt."""
    if not reactor_ready.wait(timeout=timeout):
        logger.error("❌ Timeout: reactor non prêt")
        raise TimeoutError("Reactor non initialisé")
    logger.info("✅ Reactor prêt")


# ==============================
# FONCTION DE SCRAPING
# ==============================
def scrape_and_save(sync_type='manual', scheduled_datetime=None):
    """Effectue le scraping et enregistre."""
    
    # Vérifier le lock
    if not scraping_lock.acquire(blocking=False):
        logger.warning("⚠️  Scraping en cours")
        return {
            "status": "error",
            "message": "Scraping déjà en cours",
            "sync_type": sync_type
        }
    
    try:
        start_time = time.time()
        
        # Capturer la date/heure de début si scheduled
        if scheduled_datetime is None:
            scraping_datetime = datetime.now()
        else:
            scraping_datetime = scheduled_datetime
        
        logger.info("="*80)
        logger.info(f"🚀 EXTRACTION ({sync_type}) - {scraping_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("="*80)
        
        # Attendre que le reactor soit prêt
        try:
            wait_for_reactor(timeout=30)
        except TimeoutError as e:
            logger.error(f"❌ {e}")
            return {
                "status": "error",
                "message": str(e),
                "sync_type": sync_type
            }
        
        # Résultat du scraping
        result_data = {"completed": False, "result": None, "lock": Lock()}
        
        def result_callback(result):
            """Callback quand le spider termine."""
            with result_data["lock"]:
                result_data["result"] = result
                result_data["completed"] = True
        
        # Fonction de crawl
        @defer.inlineCallbacks
        def crawl():
            try:
                global runner
                if runner is None:
                    configure_logging({'LOG_ENABLED': False})
                    runner = CrawlerRunner(ShmetSpider.custom_settings)
                
                yield runner.crawl(ShmetSpider, result_callback=result_callback)
                logger.info("✅ Crawl terminé")
            except Exception as e:
                logger.error(f"❌ Erreur crawl: {e}")
                result_callback({
                    "data": {target: None for target in TARGETS},
                    "url": URL_BASE,
                    "timestamp": datetime.now().isoformat(),
                    "error": str(e)
                })
        
        # Planifier le crawl dans le reactor
        reactor.callFromThread(crawl)
        logger.info("📤 Crawl planifié")
        
        # Attendre le résultat
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
                logger.error(f"⏱️  TIMEOUT {duration:.2f}s")
                log_sync_operation(sync_type, 'failed', 0, 'Timeout', duration)
                return {
                    "status": "error",
                    "message": f"Timeout après {duration:.2f}s",
                    "sync_type": sync_type
                }
            
            result = result_data["result"]
        
        # Traiter résultat
        if "error" in result:
            duration = time.time() - start_time
            error_msg = result.get("error")
            logger.error(f"❌ {error_msg}")
            log_sync_operation(sync_type, 'failed', 0, error_msg, duration)
            return {
                "status": "error",
                "message": error_msg,
                "sync_type": sync_type
            }
        
        # Sauvegarder
        data = result.get("data", {})
        metals_updated = save_prices_to_db(data, result.get("url"), scraping_datetime)
        
        duration = time.time() - start_time
        
        if metals_updated == len(TARGETS):
            status = 'success'
        elif metals_updated > 0:
            status = 'partial'
        else:
            status = 'failed'
        
        log_sync_operation(sync_type, status, metals_updated, None, duration)
        
        logger.info("="*80)
        logger.info(f"✅ TERMINÉ ({duration:.2f}s) - {metals_updated}/{len(TARGETS)}")
        logger.info(f"   📅 Date enregistrement: {scraping_datetime}")
        logger.info("="*80)
        
        return {
            "status": "success",
            "data": data,
            "metals_saved": metals_updated,
            "total_targets": len(TARGETS),
            "duration": duration,
            "sync_type": sync_type,
            "timestamp": result.get("timestamp")
        }
        
    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"❌ Erreur: {e}")
        import traceback
        logger.error(traceback.format_exc())
        
        log_sync_operation(sync_type, 'failed', 0, str(e), duration)
        
        return {
            "status": "error",
            "message": str(e),
            "sync_type": sync_type
        }
        
    finally:
        scraping_lock.release()


# ==============================
# TÂCHE PLANIFIÉE
# ==============================
def scheduled_scraping_job():
    """Tâche planifiée - exécutée dans un thread séparé."""
    # Capturer l'heure EXACTE du déclenchement du scheduler
    scheduled_time = datetime.now()
    
    logger.info("="*80)
    logger.info(f"⏰ TÂCHE PLANIFIÉE DÉCLENCHÉE - {scheduled_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*80)
    
    # Exécuter dans un thread pour ne pas bloquer le scheduler
    def run_scraping():
        try:
            # Passer la date/heure du scheduler au scraping
            result = scrape_and_save(sync_type='scheduled', scheduled_datetime=scheduled_time)
            logger.info(f"📊 Résultat scheduled: {result.get('status')}")
        except Exception as e:
            logger.error(f"❌ Erreur scheduled: {e}")
    
    thread = Thread(target=run_scraping, daemon=True, name="ScheduledScraping")
    thread.start()


# ==============================
# APPLICATION FLASK
# ==============================
app = Flask(__name__)

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

swagger = Swagger(app, config=swagger_config)

# Scheduler - sera démarré après le reactor
scheduler = None


# ==============================
# ROUTES API
# ==============================
@app.route("/", methods=["GET"])
def home():
    return jsonify({
        "service": "API extraction prix métaux",
        "version": "3.1-SCHEDULER-STABLE",
        "endpoints": {
            "/extract": "POST - Extraction manuelle",
            "/prices/latest": "GET - Derniers prix",
            "/prices/history": "GET - Historique",
            "/sync/logs": "GET - Logs",
            "/health": "GET - Santé",
            "/targets": "GET - Produits",
            "/docs": "GET - Documentation"
        }
    })

@app.route("/health", methods=["GET"])
def health_check():
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
    return jsonify({
        "targets": TARGETS,
        "count": len(TARGETS),
        "mapping": METAL_MAPPING
    })

@app.route("/extract", methods=["POST"])
def extract_prices():
    logger.info("🎯 /extract (manuel)")
    result = scrape_and_save(sync_type='manual')
    
    if result["status"] == "success":
        return jsonify(result), 200
    else:
        return jsonify(result), 500

@app.route("/prices/latest", methods=["GET"])
def get_latest_prices():
    metal_type = request.args.get("metal_type")
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        if metal_type:
            query = "SELECT * FROM metal_prices WHERE metal_type = %s ORDER BY created_at DESC LIMIT 10"
            cursor.execute(query, (metal_type,))
        else:
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
    reactor_thread = Thread(target=start_reactor, daemon=True, name="ReactorThread")
    reactor_thread.start()
    logger.info("✅ Thread reactor lancé")
    
    # 2. Attendre que le reactor soit prêt
    wait_for_reactor(timeout=30)
    
    # 3. Démarrer le scheduler APRÈS le reactor
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        func=scheduled_scraping_job,
        trigger=CronTrigger(hour=9, minute=10),  # 9h10
        id='daily_scraping',
        name='Extraction quotidienne 9h10',
        replace_existing=True
    )
    scheduler.start()
    logger.info("⏰ Scheduler démarré: extraction à 9h10")
    
    logger.info("✅ Initialisation terminée")


# ==============================
# POINT D'ENTRÉE
# ==============================
if __name__ == "__main__":
    logger.info("="*80)
    logger.info("🚀 DÉMARRAGE SERVEUR")
    logger.info("="*80)
    
    # Initialiser reactor et scheduler
    initialize_app()
    
    logger.info("📊 Documentation: http://localhost:5000/docs")
    logger.info(f"🎯 {len(TARGETS)} produits")
    logger.info("="*80)
    
    try:
        app.run(host="0.0.0.0", port=5000, debug=False, threaded=True, use_reloader=False)
    except (KeyboardInterrupt, SystemExit):
        if scheduler:
            scheduler.shutdown()
        logger.info("🛑 Arrêt")
    except Exception as e:
        logger.error(f"❌ Erreur fatale: {e}")
        raise
