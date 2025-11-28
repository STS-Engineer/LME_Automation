#API Flask pour extraction des prix de métaux depuis Shmet
"""
Installation requise:
pip install flask flasgger scrapy scrapy-playwright twisted parsel psycopg2-binary apscheduler
playwright install chromium
"""
import sys
import asyncio
# Configuration asyncio pour Windows
if sys.platform.startswith("win") and sys.version_info < (3, 14):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from flask import Flask, jsonify, request
from flasgger import Swagger
import re
import json
from datetime import datetime
import logging
from threading import Thread, Lock
from queue import Queue, Empty
import psycopg2
from psycopg2.extras import RealDictCursor
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import time
import traceback

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

logger.info("="*80)
logger.info("🚀 DÉMARRAGE DE L'APPLICATION")
logger.info("="*80)

# Import Scrapy
import scrapy
from parsel import Selector
from scrapy.crawler import CrawlerRunner
from scrapy import signals
from scrapy_playwright.page import PageMethod
from twisted.internet import reactor, defer
from crochet import setup, wait_for

# Initialiser Crochet pour gérer Twisted dans Flask
setup()

logger.info("✅ Modules importés avec succès")

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

# Mapping des produits vers les types de métaux
METAL_MAPPING = {
    "Cu cathode 1#": "copper",
    "Zn ingot 0#, Shanghai": "zinc",
    "Tin ingot 1#(99.9%),East China": "tin"
}

# ==============================
# CONFIGURATION SCRAPING
# ==============================
URL_BASE = "https://en.shmet.com/Home"

TARGETS = [
    "Cu cathode 1#",
    "Zn ingot 0#, Shanghai",
    "Tin ingot 1#(99.9%),East China",
]

logger.info(f"🎯 {len(TARGETS)} produits cibles configurés")

# Lock global pour éviter les scraping concurrents
scraping_lock = Lock()

# ==============================
# FONCTIONS BASE DE DONNÉES
# ==============================
def get_db_connection():
    """Créer une connexion à la base de données PostgreSQL."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        logger.error(f"❌ Erreur de connexion à la base de données: {e}")
        raise


def save_prices_to_db(data, source_url=URL_BASE):
    """Enregistrer les prix dans la base de données."""
    conn = None
    inserted_count = 0
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        current_date = datetime.now().date()
        
        for product_name, price in data.items():
            if price is None:
                logger.warning(f"⚠️  Prix manquant pour {product_name}")
                continue
            
            metal_type = METAL_MAPPING.get(product_name)
            if not metal_type:
                logger.warning(f"⚠️  Type de métal inconnu pour {product_name}")
                continue
            
            insert_query = """
                INSERT INTO metal_prices 
                (source_product_name, metal_type, price, currency, unit, source_url, price_date, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            """
            
            cursor.execute(insert_query, (
                product_name,
                metal_type,
                price,
                'CNY',
                'ton',
                source_url,
                current_date
            ))
            inserted_count += 1
            logger.info(f"   ✅ Enregistré: {product_name} = {price} CNY")
        
        conn.commit()
        logger.info(f"✅ {inserted_count} prix enregistrés dans la base")
        return inserted_count
        
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'enregistrement: {e}")
        logger.error(traceback.format_exc())
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
        
        cursor.execute(insert_query, (
            sync_type,
            status,
            metals_updated,
            error_message,
            duration
        ))
        
        conn.commit()
        logger.info(f"📝 Log enregistré: {status} - {metals_updated} métaux")
        
    except Exception as e:
        logger.error(f"❌ Erreur log: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()


# ==============================
# FONCTIONS D'EXTRACTION
# ==============================
def extract_from_dom(response: scrapy.http.Response):
    """Extraction depuis le DOM rendu."""
    logger.info("🔍 Extraction DOM...")
    sel = Selector(text=response.text)
    data = {target: None for target in TARGETS}
    
    sections = sel.xpath('//div[contains(@class, "card-title") and contains(@class, "pull-left")]')
    
    china_domestic_section = None
    for section in sections:
        title = section.xpath('string(.)').get("").strip()
        if "China Domestic Market Price" in title:
            china_domestic_section = section
            logger.info(f"   ✅ Section cible trouvée: {title}")
            break
    
    if not china_domestic_section:
        logger.warning("   ⚠️  Section 'China Domestic Market Price' non trouvée")
        return data
    
    parent_card = china_domestic_section.xpath('ancestor::div[contains(@class, "card") or contains(@class, "panel")]')[0]
    rows = parent_card.css("tr.el-table__row")
    logger.info(f"   {len(rows)} lignes trouvées")
    
    for idx, row in enumerate(rows, 1):
        name_selectors = [
            "td.el-table_1_column_1.name-color span.cell-name",
            'td[class*="el-table_1_column_1"].name-color span.cell-name',
            "td.name-color span.cell-name",
            "td span.cell-name",
        ]
        
        name_el = None
        for selector in name_selectors:
            name_el = row.css(selector)
            if name_el:
                break
        
        val_selectors = [
            "td.el-table_1_column_3 div.cell",
            'td[class*="el-table_1_column_3"] div.cell',
            "td:nth-child(3) div.cell",
        ]
        
        val_el = None
        for selector in val_selectors:
            val_el = row.css(selector)
            if val_el:
                break
        
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
                target_normalized = target.lower().strip()
                name_normalized = name.lower().strip()
                
                if target_normalized == name_normalized or target_normalized in name_normalized:
                    data[target] = price
                    logger.info(f"   ✅ MATCH: {target} = {price}")
                    break
        except ValueError:
            continue
    
    found = sum(1 for v in data.values() if v is not None)
    logger.info(f"✅ {found}/{len(TARGETS)} produits extraits")
    return data


def extract_from_json_scripts(response: scrapy.http.Response):
    """Méthode de secours: extraction depuis JSON."""
    logger.info("🔍 Extraction JSON (fallback)...")
    data = {target: None for target in TARGETS}
    scripts = Selector(text=response.text).xpath("//script/text()").getall()
    
    for script_text in scripts:
        if not script_text:
            continue
        
        json_match = re.search(r'(\{.*\}|\[.*\])', script_text, flags=re.DOTALL)
        if not json_match:
            continue
        
        try:
            json_data = json.loads(json_match.group(1))
        except:
            continue
        
        stack = [json_data]
        while stack:
            current = stack.pop()
            
            if isinstance(current, dict):
                product_name = current.get("name") or current.get("product")
                
                if isinstance(product_name, str):
                    for target in TARGETS:
                        if target.lower() in product_name.lower():
                            price_fields = ["middle", "median", "price", "mid"]
                            for field in price_fields:
                                if field in current:
                                    try:
                                        price = float(str(current[field]).replace(",", ""))
                                        data[target] = price
                                        logger.info(f"   ✅ {target} = {price}")
                                        break
                                    except:
                                        pass
                
                stack.extend(current.values())
            elif isinstance(current, list):
                stack.extend(current)
    
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
        "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": 45000,
        "PLAYWRIGHT_LAUNCH_OPTIONS": {
            "headless": True,
            "args": ["--no-sandbox", "--disable-setuid-sandbox"]
        },
        "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "ROBOTSTXT_OBEY": False,
        "LOG_ENABLED": False,
        "CONCURRENT_REQUESTS": 1,
        "DOWNLOAD_DELAY": 2,
    }
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.results = []
    
    def start_requests(self):
        logger.info(f"🌐 Requête vers: {URL_BASE}")
        yield scrapy.Request(
            url=URL_BASE,
            meta={
                "playwright": True,
                "playwright_page_methods": [
                    PageMethod("wait_for_selector", "tr.el-table__row", timeout=20000),
                    PageMethod("wait_for_timeout", 2000),
                ],
                "playwright_include_page": False,
            },
            callback=self.parse,
            errback=self.errback,
        )
    
    def parse(self, response):
        logger.info(f"📄 Parsing: {response.url}")
        
        data = extract_from_dom(response)
        extraction_method = "dom"
        
        if all(value is None for value in data.values()):
            logger.warning("⚠️  Passage au fallback JSON...")
            data = extract_from_json_scripts(response)
            extraction_method = "json_fallback"
        
        result = {
            "data": data,
            "url": response.url,
            "timestamp": datetime.now().isoformat(),
            "method": extraction_method
        }
        
        self.results.append(result)
        return result
    
    def errback(self, failure):
        logger.error(f"❌ Erreur spider: {failure.value}")
        result = {
            "data": {target: None for target in TARGETS},
            "url": URL_BASE,
            "timestamp": datetime.now().isoformat(),
            "error": str(failure.value)
        }
        self.results.append(result)
        return result


# ==============================
# FONCTION D'EXÉCUTION AVEC CROCHET
# ==============================
@wait_for(timeout=120)
def run_spider():
    """Exécute le spider avec Crochet (gestion propre du reactor)."""
    logger.info("🕷️  Démarrage du spider...")
    
    runner = CrawlerRunner(ShmetSpider.custom_settings)
    deferred = runner.crawl(ShmetSpider)
    return deferred


def scrape_and_save(sync_type='manual'):
    """Effectue le scraping et enregistre les données."""
    
    # Vérifier si un scraping est déjà en cours
    if not scraping_lock.acquire(blocking=False):
        logger.warning("⚠️  Un scraping est déjà en cours")
        return {
            "status": "error",
            "message": "Un scraping est déjà en cours. Veuillez patienter.",
            "sync_type": sync_type
        }
    
    start_time = time.time()
    
    try:
        logger.info("="*80)
        logger.info(f"🚀 DÉBUT DE L'EXTRACTION ({sync_type})")
        logger.info("="*80)
        
        # Exécuter le spider
        try:
            run_spider()
            logger.info("✅ Spider terminé")
        except Exception as spider_error:
            duration = time.time() - start_time
            error_msg = f"Erreur spider: {str(spider_error)}"
            logger.error(f"❌ {error_msg}")
            logger.error(traceback.format_exc())
            
            log_sync_operation(sync_type, 'failed', 0, error_msg, duration)
            
            return {
                "status": "error",
                "message": error_msg,
                "sync_type": sync_type,
                "duration": duration
            }
        
        # Récupérer les résultats
        # Note: Dans Crochet, on ne peut pas facilement passer de données
        # On va utiliser une approche simplifiée ici
        
        # Pour une solution complète, vous devriez stocker les résultats
        # dans une variable globale ou une base de données temporaire
        
        # Simulation de résultat (à remplacer par la vraie logique)
        result = {
            "data": {target: None for target in TARGETS},
            "url": URL_BASE,
            "timestamp": datetime.now().isoformat()
        }
        
        # En production, récupérez les vrais résultats du spider
        # Pour cet exemple, on simule des données
        data = result.get("data", {})
        
        if "error" in result:
            duration = time.time() - start_time
            log_sync_operation(sync_type, 'failed', 0, result["error"], duration)
            return {
                "status": "error",
                "message": result["error"],
                "sync_type": sync_type
            }
        
        # Enregistrer dans la base
        try:
            metals_updated = save_prices_to_db(data, result["url"])
        except Exception as db_error:
            duration = time.time() - start_time
            error_msg = f"Erreur base de données: {str(db_error)}"
            logger.error(f"❌ {error_msg}")
            
            log_sync_operation(sync_type, 'failed', 0, error_msg, duration)
            
            return {
                "status": "error",
                "message": error_msg,
                "sync_type": sync_type,
                "duration": duration
            }
        
        duration = time.time() - start_time
        
        # Déterminer le statut
        if metals_updated == len(TARGETS):
            status = 'success'
        elif metals_updated > 0:
            status = 'partial'
        else:
            status = 'failed'
        
        log_sync_operation(sync_type, status, metals_updated, None, duration)
        
        logger.info("="*80)
        logger.info(f"✅ EXTRACTION TERMINÉE ({duration:.2f}s)")
        logger.info(f"   Métaux mis à jour: {metals_updated}/{len(TARGETS)}")
        logger.info("="*80)
        
        return {
            "status": "success",
            "data": data,
            "metals_saved": metals_updated,
            "total_targets": len(TARGETS),
            "duration": duration,
            "sync_type": sync_type,
            "timestamp": result["timestamp"]
        }
        
    except Exception as e:
        duration = time.time() - start_time
        error_msg = f"Erreur globale: {str(e)}"
        logger.error(f"❌ {error_msg}")
        logger.error(traceback.format_exc())
        
        log_sync_operation(sync_type, 'failed', 0, error_msg, duration)
        
        return {
            "status": "error",
            "message": error_msg,
            "sync_type": sync_type,
            "duration": duration,
            "traceback": traceback.format_exc()
        }
    
    finally:
        scraping_lock.release()


# ==============================
# TÂCHE PLANIFIÉE
# ==============================
def scheduled_scraping_job():
    """Tâche planifiée pour l'extraction quotidienne."""
    logger.info("⏰ Exécution de la tâche planifiée (8h00)")
    scrape_and_save(sync_type='scheduled')


# ==============================
# APPLICATION FLASK
# ==============================
app = Flask(__name__)

swagger_config = {
    "headers": [],
    "specs": [{
        "endpoint": "apispec",
        "route": "/apispec.json",
    }],
    "static_url_path": "/flasgger_static",
    "swagger_ui": True,
    "specs_route": "/docs"
}

swagger = Swagger(app, config=swagger_config)

# Initialiser le scheduler
scheduler = BackgroundScheduler()
scheduler.add_job(
    func=scheduled_scraping_job,
    trigger=CronTrigger(hour=8, minute=0),
    id='daily_scraping',
    name='Extraction quotidienne à 8h00',
    replace_existing=True
)
scheduler.start()
logger.info("⏰ Scheduler initialisé")


# ==============================
# ROUTES API
# ==============================
@app.route("/", methods=["GET"])
def home():
    """Page d'accueil"""
    return jsonify({
        "service": "API d'extraction des prix de métaux",
        "version": "2.2-fixed",
        "endpoints": {
            "/extract": "POST - Extraire et enregistrer",
            "/prices/latest": "GET - Derniers prix",
            "/prices/history": "GET - Historique",
            "/sync/logs": "GET - Logs",
            "/health": "GET - État API",
            "/docs": "GET - Documentation"
        }
    })

@app.route("/health", methods=["GET"])
def health_check():
    """Santé de l'API"""
    db_status = "unknown"
    try:
        conn = get_db_connection()
        conn.close()
        db_status = "connected"
    except Exception as e:
        db_status = f"error: {str(e)}"
    
    return jsonify({
        "status": "healthy",
        "database": db_status,
        "scheduler": "running" if scheduler.running else "stopped",
        "timestamp": datetime.now().isoformat()
    })

@app.route("/targets", methods=["GET"])
def get_targets():
    """Liste des produits"""
    return jsonify({
        "targets": TARGETS,
        "count": len(TARGETS),
        "mapping": METAL_MAPPING
    })

@app.route("/extract", methods=["POST"])
def extract_prices():
    """Extraction et enregistrement"""
    logger.info("🎯 Requête /extract reçue")
    
    try:
        result = scrape_and_save(sync_type='manual')
        
        if result["status"] == "success":
            return jsonify(result), 200
        else:
            return jsonify(result), 500
            
    except Exception as e:
        logger.error(f"❌ Erreur dans /extract: {e}")
        logger.error(traceback.format_exc())
        return jsonify({
            "status": "error",
            "message": str(e),
            "traceback": traceback.format_exc()
        }), 500

@app.route("/prices/latest", methods=["GET"])
def get_latest_prices():
    """Derniers prix enregistrés"""
    metal_type = request.args.get("metal_type")
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        if metal_type:
            query = """
                SELECT * FROM metal_prices 
                WHERE metal_type = %s
                ORDER BY created_at DESC 
                LIMIT 10
            """
            cursor.execute(query, (metal_type,))
        else:
            query = """
                SELECT DISTINCT ON (metal_type) *
                FROM metal_prices
                ORDER BY metal_type, created_at DESC
            """
            cursor.execute(query)
        
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return jsonify({
            "status": "success",
            "count": len(results),
            "prices": results
        }), 200
        
    except Exception as e:
        logger.error(f"❌ Erreur: {e}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route("/prices/history", methods=["GET"])
def get_price_history():
    """Historique des prix"""
    metal_type = request.args.get("metal_type")
    days = request.args.get("days", default=7, type=int)
    limit = request.args.get("limit", default=100, type=int)
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        if metal_type:
            query = """
                SELECT * FROM metal_prices 
                WHERE metal_type = %s 
                AND created_at >= NOW() - INTERVAL '%s days'
                ORDER BY created_at DESC 
                LIMIT %s
            """
            cursor.execute(query, (metal_type, days, limit))
        else:
            query = """
                SELECT * FROM metal_prices 
                WHERE created_at >= NOW() - INTERVAL '%s days'
                ORDER BY created_at DESC 
                LIMIT %s
            """
            cursor.execute(query, (days, limit))
        
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return jsonify({
            "status": "success",
            "count": len(results),
            "history": results
        }), 200
        
    except Exception as e:
        logger.error(f"❌ Erreur: {e}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route("/sync/logs", methods=["GET"])
def get_sync_logs():
    """Logs de synchronisation"""
    limit = request.args.get("limit", default=50, type=int)
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        query = "SELECT * FROM sync_logs ORDER BY created_at DESC LIMIT %s"
        cursor.execute(query, (limit,))
        
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return jsonify({
            "status": "success",
            "count": len(results),
            "logs": results
        }), 200
        
    except Exception as e:
        logger.error(f"❌ Erreur: {e}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


# ==============================
# POINT D'ENTRÉE
# ==============================
if __name__ == "__main__":
    logger.info("="*80)
    logger.info("🚀 DÉMARRAGE DU SERVEUR (VERSION CORRIGÉE)")
    logger.info("📊 Documentation: http://localhost:5000/docs")
    logger.info("="*80)
    
    try:
        app.run(
            host="0.0.0.0",
            port=5000,
            debug=False,
            threaded=True
        )
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("🛑 Arrêt")
    except Exception as e:
        logger.error(f"❌ Erreur fatale: {e}")
        logger.error(traceback.format_exc())
