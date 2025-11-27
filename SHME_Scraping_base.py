# API Flask pour extraction des prix de métaux depuis Shmet
"""
Installation requise:
pip install flask flasgger scrapy scrapy-playwright twisted[tls] pyopenssl service_identity parsel psycopg2-binary apscheduler
playwright install chromium
"""
import sys
import asyncio

# ==============================
# FIXATION 1: INSTALLATION DU REACTOR ASYNCIO
# Twisted/Scrapy DOIT utiliser le reactor asyncio pour être compatible avec Playwright
# Ceci DOIT être fait avant tout import de Twisted ou Scrapy
# ==============================
try:
    from twisted.internet import asyncioreactor
    asyncioreactor.install()
    print("✅ Reactor asyncio de Twisted installé avec succès.")
except Exception as e:
    # Peut arriver si Twisted est déjà importé, mais l'ordre ici est correct.
    print(f"⚠️  Erreur d'installation du reactor: {e}")
    
# Configuration asyncio pour Windows (seulement pour Python < 3.14)
if sys.platform.startswith("win") and sys.version_info < (3, 14):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from flask import Flask, jsonify, request
from flasgger import Swagger
import re
import json
from datetime import datetime
import logging
from threading import Thread
from queue import Queue
import psycopg2
from psycopg2.extras import RealDictCursor
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import time

# Import Scrapy APRES l'installation du reactor
import scrapy
from parsel import Selector
# FIXATION 2: Import de CrawlerRunner au lieu de CrawlerProcess
from scrapy.crawler import CrawlerRunner
from scrapy import signals
from scrapy.utils.log import configure_logging # Utile pour Scrapy
from scrapy_playwright.page import PageMethod
from twisted.internet import reactor # Import du reactor nécessaire pour le run_spider_in_thread

# ==============================
# CONFIGURATION DU LOGGING
# ==============================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

logger.info("="*80)
logger.info("🚀 DÉMARRAGE DE L'APPLICATION")
logger.info("="*80)

logger.info("✅ Modules importés avec succès")

# ==============================
# CONFIGURATION BASE DE DONNÉES
# ==============================
# ATTENTION: Remplacer ces valeurs par des variables d'environnement en production
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


# ==============================
# FONCTIONS BASE DE DONNÉES
# ==============================
def get_db_connection():
    """Créer une connexion à la base de données PostgreSQL."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("✅ Connexion à la base de données établie")
        return conn
    except Exception as e:
        logger.error(f"❌ Erreur de connexion à la base de données: {e}")
        raise


def save_prices_to_db(data, source_url=URL_BASE):
    """
    Enregistrer les prix dans la base de données.
    
    Args:
        data: Dictionnaire {product_name: price}
        source_url: URL source des données
    
    Returns:
        Nombre d'enregistrements insérés
    """
    conn = None
    inserted_count = 0
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Date actuelle pour price_date
        current_date = datetime.now().date()
        
        for product_name, price in data.items():
            if price is None:
                logger.warning(f"⚠️  Prix manquant pour {product_name}, non enregistré")
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
            logger.info(f"    ✅ Enregistré: {product_name} = {price} CNY (date: {current_date})")
        
        conn.commit()
        logger.info(f"✅ {inserted_count} prix enregistrés dans la base de données")
        return inserted_count
        
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'enregistrement: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            # S'assurer que le curseur est fermé avant la connexion
            if 'cursor' in locals() and cursor:
                cursor.close()
            conn.close()
            logger.info("🔒 Connexion à la base de données fermée")


def log_sync_operation(sync_type, status, metals_updated, error_message=None, duration=None):
    """
    Enregistrer une opération de synchronisation dans sync_logs.
    
    Args:
        sync_type: 'scheduled', 'manual', 'api'
        status: 'success', 'partial', 'failed'
        metals_updated: Nombre de métaux mis à jour
        error_message: Message d'erreur (optionnel)
        duration: Durée en secondes (optionnel)
    """
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
        logger.info(f"📝 Log de synchronisation enregistré: {status} - {metals_updated} métaux")
        
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'enregistrement du log: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            if 'cursor' in locals() and cursor:
                cursor.close()
            conn.close()


# ==============================
# FONCTIONS D'EXTRACTION
# ==============================
def extract_from_dom(response: scrapy.http.Response):
    """Extraction depuis le DOM rendu - Section China Domestic Market Price."""
    logger.info("🔍 Extraction DOM...")
    sel = Selector(text=response.text)
    data = {target: None for target in TARGETS}
    
    # Chercher la section "China Domestic Market Price (GMT+8)"
    sections = sel.xpath('//div[contains(@class, "card-title") and contains(@class, "pull-left")]')
    
    china_domestic_section = None
    for section in sections:
        title = section.xpath('string(.)').get("").strip()
        logger.info(f"    Section trouvée: {title}")
        if "China Domestic Market Price" in title:
            china_domestic_section = section
            logger.info(f"    ✅ Section cible trouvée: {title}")
            break
    
    if not china_domestic_section:
        logger.warning("    ⚠️  Section 'China Domestic Market Price' non trouvée")
        return data
    
    # Remonter au conteneur parent contenant le tableau
    parent_card = china_domestic_section.xpath('ancestor::div[contains(@class, "card") or contains(@class, "panel")]')[0]
    
    # Chercher les lignes du tableau dans cette section
    rows = parent_card.css("tr.el-table__row")
    logger.info(f"    {len(rows)} lignes trouvées dans la section China Domestic Market")
    
    for idx, row in enumerate(rows, 1):
        # Extraction du nom du produit
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
        
        # Extraction du prix (colonne 3 généralement)
        val_selectors = [
            "td.el-table_1_column_3 div.cell",
            'td[class*="el-table_1_column_3"] div.cell',
            "td:nth-child(3) div.cell",
            "td div.cell",
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
        
        logger.info(f"    Ligne {idx}: Produit='{name}' | Prix brut='{raw_value}'")
        
        # Nettoyage et conversion
        clean_value = raw_value.replace(",", "")
        numeric_value = re.sub(r"[^\d\.\-]", "", clean_value)
        
        if not numeric_value:
            continue
        
        try:
            price = float(numeric_value)
            
            # Matching exact avec les cibles
            for target in TARGETS:
                # Normaliser pour comparaison
                target_normalized = target.lower().strip()
                name_normalized = name.lower().strip()
                
                # Match exact ou contient le nom complet
                if target_normalized == name_normalized or target_normalized in name_normalized:
                    data[target] = price
                    logger.info(f"    ✅ MATCH: {target} = {price}")
                    break
        except ValueError as e:
            logger.warning(f"    ⚠️  Erreur conversion: {e}")
            continue
    
    found = sum(1 for v in data.values() if v is not None)
    logger.info(f"✅ {found}/{len(TARGETS)} produits extraits")
    return data


def extract_from_json_scripts(response: scrapy.http.Response):
    """Méthode de secours: extraction depuis JSON embarqué."""
    logger.info("🔍 Extraction JSON (fallback)...")
    data = {target: None for target in TARGETS}
    scripts = Selector(text=response.text).xpath("//script/text()").getall()
    
    for script_text in scripts:
        if not script_text:
            continue
        
        # Tente de trouver un objet ou un tableau JSON dans le script
        json_match = re.search(r'(\{.*\}|\[.*\])', script_text, flags=re.DOTALL)
        if not json_match:
            continue
        
        try:
            json_data = json.loads(json_match.group(1))
        except:
            continue
        
        # Exploration récursive
        stack = [json_data]
        while stack:
            current = stack.pop()
            
            if isinstance(current, dict):
                product_name = current.get("name") or current.get("product") or current.get("title")
                
                if isinstance(product_name, str):
                    for target in TARGETS:
                        if target.lower() in product_name.lower():
                            price_fields = ["middle", "median", "price", "mid", "value", "amount"]
                            for field in price_fields:
                                if field in current:
                                    try:
                                        price = float(str(current[field]).replace(",", ""))
                                        data[target] = price
                                        logger.info(f"    ✅ {target} = {price}")
                                        break
                                    except (ValueError, TypeError):
                                        pass
                            
                # Ajouter les valeurs des dictionnaires à la pile
                stack.extend([v for v in current.values() if isinstance(v, (dict, list))])
            elif isinstance(current, list):
                # Ajouter les éléments des listes à la pile
                stack.extend(current)
    
    found = sum(1 for v in data.values() if v is not None)
    logger.info(f"✅ {found}/{len(TARGETS)} produits extraits (JSON fallback)")
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
        "LOG_ENABLED": True,
        "LOG_LEVEL": "INFO",
        "CONCURRENT_REQUESTS": 1,
        "DOWNLOAD_DELAY": 2,
    }
    
    def __init__(self, result_queue=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.result_queue = result_queue
    
    def start_requests(self):
        logger.info(f"🌐 Requête vers: {URL_BASE}")
        yield scrapy.Request(
            url=URL_BASE,
            meta={
                "playwright": True,
                "playwright_page_methods": [
                    # Attendre que la table de prix soit chargée
                    PageMethod("wait_for_selector", "tr.el-table__row", timeout=20000), 
                    # Délai supplémentaire pour s'assurer que JS est stable
                    PageMethod("wait_for_timeout", 2000), 
                ],
                "playwright_include_page": False,
            },
            callback=self.parse,
            errback=self.errback,
        )
    
    def parse(self, response):
        """Parse la réponse."""
        logger.info(f"📄 Parsing: {response.url} ({len(response.text)} chars)")
        
        # Extraction DOM
        data = extract_from_dom(response)
        extraction_method = "dom"
        
        # Fallback JSON si nécessaire (si aucun prix n'a été trouvé par le DOM)
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
        
        # Place le résultat dans la queue pour la fonction parente (scrape_and_save)
        if self.result_queue:
            self.result_queue.put(result)
        
        # Scrapy doit yield quelque chose même si ce n'est pas utilisé
        yield result
    
    def errback(self, failure):
        """Gestion des erreurs."""
        logger.error(f"❌ Erreur Scrapy/Playwright: {failure.value}")
        result = {
            "data": {target: None for target in TARGETS},
            "url": URL_BASE,
            "timestamp": datetime.now().isoformat(),
            "error": str(failure.value)
        }
        
        if self.result_queue:
            self.result_queue.put(result)
        
        yield result


# ==============================
# FONCTION D'EXÉCUTION DU SPIDER
# ==============================
def run_spider_in_thread(result_queue):
    """Exécute le spider dans un thread séparé avec CrawlerRunner compatible asyncio."""
    try:
        # FIXATION CRITIQUE: Créer un nouvel event loop pour ce thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        logger.info("✅ Event loop asyncio créé pour le thread")
        
        configure_logging(ShmetSpider.custom_settings)
        
        runner = CrawlerRunner(ShmetSpider.custom_settings)
        
        # Préparer le crawl (une tâche asyncio/Twisted)
        deferred = runner.crawl(ShmetSpider, result_queue=result_queue)
        
        logger.info("🕷️  Démarrage du reactor et du spider dans le thread...")
        
        # Arrêter le reactor quand le crawl est fini
        deferred.addBoth(lambda _: reactor.stop())
        
        # Bloque jusqu'à ce que deferred soit terminé
        reactor.run(installSignalHandlers=False)
        
        logger.info("✅ Reactor arrêté et Spider terminé")
        
    except Exception as e:
        logger.error(f"❌ Erreur fatale dans le thread du spider: {e}")
        # S'assurer de mettre une erreur dans la queue même en cas de crash critique
        result_queue.put({
            "data": {target: None for target in TARGETS},
            "url": URL_BASE,
            "timestamp": datetime.now().isoformat(),
            "error": "Erreur critique du reactor ou du thread: " + str(e)
        })
    finally:
        # Nettoyer l'event loop après utilisation
        try:
            # Récupérer le loop créé dans ce thread
            loop = asyncio.get_event_loop() 
            if not loop.is_closed():
                loop.close()
            logger.info("🧹 Event loop fermé proprement")
        except Exception as cleanup_error:
            logger.warning(f"⚠️  Erreur lors du nettoyage de l'event loop: {cleanup_error}")


def scrape_and_save(sync_type='manual'):
    """
    Effectue le scraping et enregistre les données dans la base.
    
    Args:
        sync_type: 'scheduled', 'manual', 'api'
    
    Returns:
        dict: Résultat de l'opération
    """
    start_time = time.time()
    
    logger.info("="*80)
    logger.info(f"🚀 DÉBUT DE L'EXTRACTION ({sync_type})")
    logger.info("="*80)
    
    try:
        # Queue pour récupérer le résultat
        result_queue = Queue()
        
        # Lancer le spider dans un thread séparé
        spider_thread = Thread(
            target=run_spider_in_thread,
            args=(result_queue,),
            daemon=True
        )
        spider_thread.start()
        
        # Attendre le résultat avec timeout
        timeout_duration = 120 # 2 minutes
        try:
            result = result_queue.get(timeout=timeout_duration)
            logger.info("✅ Résultat reçu du spider")
        except:
            duration = time.time() - start_time
            logger.error(f"⏱️  Timeout après {timeout_duration}s")
            log_sync_operation(sync_type, 'failed', 0, 'Timeout', duration)
            return {
                "status": "error",
                "message": "Timeout lors du scraping",
                "sync_type": sync_type
            }
        
        # Enregistrer dans la base de données
        if "error" in result:
            duration = time.time() - start_time
            log_sync_operation(sync_type, 'failed', 0, result.get("error"), duration)
            return {
                "status": "error",
                "message": result.get("error"),
                "sync_type": sync_type
            }
        
        data = result.get("data", {})
        metals_updated = save_prices_to_db(data, result.get("url"))
        
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
        logger.info(f"    Métaux mis à jour: {metals_updated}/{len(TARGETS)}")
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
        logger.error(f"❌ Erreur globale: {e}")
        
        log_sync_operation(sync_type, 'failed', 0, str(e), duration)
        
        return {
            "status": "error",
            "message": str(e),
            "sync_type": sync_type
        }


# ==============================
# TÂCHE PLANIFIÉE
# ==============================
def scheduled_scraping_job():
    """Tâche planifiée pour l'extraction quotidienne."""
    logger.info("⏰ Exécution de la tâche planifiée (16h30)")
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
        "rule_filter": lambda rule: True,
        "model_filter": lambda tag: True,
    }],
    "static_url_path": "/flasgger_static",
    "swagger_ui": True,
    "specs_route": "/docs"
}

swagger = Swagger(app, config=swagger_config)

# Initialiser le scheduler
scheduler = BackgroundScheduler()
# MODIFICATION CLÉ: Changement de l'heure à 16h30
scheduler.add_job(
    func=scheduled_scraping_job,
    trigger=CronTrigger(hour=16, minute=30),  # Tous les jours à 16h30
    id='daily_scraping',
    name='Extraction quotidienne des prix à 16h30',
    replace_existing=True
)
scheduler.start()
logger.info("⏰ Scheduler initialisé: extraction quotidienne à 16h30 (heure locale du serveur)")


# ==============================
# ROUTES API
# ==============================
@app.route("/", methods=["GET"])
def home():
    """
    Page d'accueil
    ---
    responses:
      200:
        description: Infos API
    """
    return jsonify({
        "service": "API d'extraction des prix de métaux",
        "version": "2.3",
        "features": {
            "scraping": "Extraction depuis Shmet via Scrapy/Playwright",
            "database": "Enregistrement PostgreSQL (metal_prices, sync_logs)",
            "scheduler": "Exécution quotidienne à 16h30"
        },
        "endpoints": {
            "/extract": "POST - Extraire et enregistrer les prix",
            "/prices/latest": "GET - Derniers prix enregistrés",
            "/prices/history": "GET - Historique des prix",
            "/sync/logs": "GET - Logs de synchronisation",
            "/health": "GET - État de l'API",
            "/targets": "GET - Produits suivis",
            "/docs": "GET - Documentation"
        }
    })

@app.route("/health", methods=["GET"])
def health_check():
    """
    Santé de l'API et connexion DB
    ---
    responses:
      200:
        description: API opérationnelle
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
        "scheduler": "running" if scheduler.running else "stopped",
        "timestamp": datetime.now().isoformat()
    })

@app.route("/targets", methods=["GET"])
def get_targets():
    """
    Liste des produits
    ---
    responses:
      200:
        description: Produits suivis
    """
    return jsonify({
        "targets": TARGETS,
        "count": len(TARGETS),
        "mapping": METAL_MAPPING
    })

@app.route("/extract", methods=["POST"])
def extract_prices():
    """
    Extraction et enregistrement des prix
    ---
    responses:
      200:
        description: Extraction réussie
      500:
        description: Erreur
    """
    logger.info("🎯 Requête /extract reçue (manuelle)")
    result = scrape_and_save(sync_type='api')
    
    if result["status"] == "success" or result["status"] == "partial":
        return jsonify(result), 200
    else:
        return jsonify(result), 500

@app.route("/prices/latest", methods=["GET"])
def get_latest_prices():
    """
    Obtenir les derniers prix enregistrés
    ---
    parameters:
      - name: metal_type
        in: query
        type: string
        required: false
        description: Filtrer par type de métal (copper, zinc, tin)
    responses:
      200:
        description: Derniers prix
    """
    metal_type = request.args.get("metal_type")
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        if metal_type:
            query = """
                -- Sélectionne les 10 dernières entrées pour un type de métal
                SELECT * FROM metal_prices 
                WHERE metal_type = %s
                ORDER BY created_at DESC 
                LIMIT 10
            """
            cursor.execute(query, (metal_type,))
        else:
            query = """
                -- Sélectionne le prix le plus récent pour chaque type de métal
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
    """
    Obtenir l'historique des prix
    ---
    parameters:
      - name: metal_type
        in: query
        type: string
        required: false
      - name: days
        in: query
        type: integer
        required: false
        default: 7
      - name: limit
        in: query
        type: integer
        required: false
        default: 100
    responses:
      200:
        description: Historique des prix
    """
    metal_type = request.args.get("metal_type")
    days = request.args.get("days", default=7, type=int)
    limit = request.args.get("limit", default=100, type=int)
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        base_query = """
            SELECT * FROM metal_prices 
            WHERE created_at >= NOW() - INTERVAL '%s days'
        """
        params = [days]
        
        if metal_type:
            base_query += " AND metal_type = %s "
            params.append(metal_type)
        
        final_query = base_query + " ORDER BY created_at DESC LIMIT %s"
        params.append(limit)

        # Les paramètres doivent être passés sous forme de tuple ou de liste
        # Pour les jours, il faut le convertir en chaîne pour la syntaxe PostgreSQL INTERVAL
        
        if metal_type:
             # Exécuter avec (days, metal_type, limit)
            cursor.execute(final_query, (str(days), metal_type, limit))
        else:
            # Exécuter avec (days, limit)
            cursor.execute(final_query, (str(days), limit))
        
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            "status": "success",
            "count": len(results),
            "filters": {
                "metal_type": metal_type,
                "days": days,
                "limit": limit
            },
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
    """
    Obtenir les logs de synchronisation
    ---
    parameters:
      - name: limit
        in: query
        type: integer
        required: false
        default: 50
    responses:
      200:
        description: Logs de synchronisation
    """
    limit = request.args.get("limit", default=50, type=int)
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        query = """
            SELECT * FROM sync_logs 
            ORDER BY created_at DESC 
            LIMIT %s
        """
        cursor.execute(query, (limit,))
        
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            "status": "success",
            "count": len(results),
            "limit": limit,
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
    # Assurer que l'event loop principal est disponible si nécessaire
    try:
        # Tenter d'obtenir le loop existant
        loop = asyncio.get_event_loop()
        if loop.is_running():
            pass
    except RuntimeError:
        # Si aucun loop n'existe, en créer un nouveau et le définir
        asyncio.set_event_loop(asyncio.new_event_loop())
        
    logger.info("="*80)
    logger.info("🚀 DÉMARRAGE DU SERVEUR FLASK")
    logger.info(f"🌐 Accès à l'API: http://0.0.0.0:5000/health")
    logger.info("📊 Documentation: http://0.0.0.0:5000/docs")
    logger.info(f"🎯 {len(TARGETS)} produits suivis")
    logger.info("🗄️ Base de données: PostgreSQL (Azure)")
    logger.info("⏰ Extraction quotidienne planifiée: 16h30 (heure locale du serveur)")
    logger.info("="*80)
    
    try:
        # Lancer l'application Flask
        app.run(
            host="0.0.0.0",
            port=5000,
            debug=False,
            threaded=True 
        )
    except (KeyboardInterrupt, SystemExit):
        # Arrêter le scheduler proprement lorsque l'application est interrompue
        if scheduler.running:
            scheduler.shutdown()
            logger.info("🛑 Arrêt du scheduler")
        logger.info("👋 Arrêt de l'application")
    except Exception as e:
        logger.error(f"❌ Erreur fatale au lancement de l'API: {e}")
        if scheduler.running:
            scheduler.shutdown()
        raise
