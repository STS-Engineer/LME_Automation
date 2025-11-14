-- ============================================
-- SCRIPT DE CRÉATION DES TABLES - LME_DB (Final)
-- ============================================

-- Suppression des tables existantes (ATTENTION : efface toutes les données)
DROP TABLE IF EXISTS sync_logs CASCADE;
DROP TABLE IF EXISTS metal_prices CASCADE;

-- ============================================
-- Table: metal_prices
-- Stocke les prix des métaux
-- ============================================
CREATE TABLE metal_prices (
    id SERIAL PRIMARY KEY,
    -- Nom exact du produit source (ex: "Cu cathode 1#")
    source_product_name VARCHAR(100) NOT NULL, 
    metal_type VARCHAR(50) NOT NULL,
    price DECIMAL(12, 2) NOT NULL,
    currency VARCHAR(10) DEFAULT 'CNY',
    unit VARCHAR(20) DEFAULT 'ton',
    -- Laisse cette colonne vide par défaut pour permettre d'autres URL
    source_url VARCHAR(500), 
    -- Colonne d'enregistrement de la date et heure d'extraction
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
    
    -- Contraintes
    CONSTRAINT check_metal_type CHECK (metal_type IN ('copper', 'zinc', 'tin')),
    CONSTRAINT check_price_positive CHECK (price > 0)
);

-- Index
CREATE INDEX idx_metal_prices_metal_type ON metal_prices(metal_type);
CREATE INDEX idx_metal_prices_created_at ON metal_prices(created_at DESC);
CREATE INDEX idx_metal_prices_composite ON metal_prices(metal_type, created_at DESC);

-- Commentaires
COMMENT ON TABLE metal_prices IS 'Historique des prix des métaux extraits.';
COMMENT ON COLUMN metal_prices.source_product_name IS 'Nom exact du produit tel qu''extrait de la source.';
COMMENT ON COLUMN metal_prices.metal_type IS 'Type de métal: copper, zinc, tin.';
COMMENT ON COLUMN metal_prices.price IS 'Prix en devise spécifiée.';
COMMENT ON COLUMN metal_prices.source_url IS 'URL complète du site source utilisé pour l''extraction.';
COMMENT ON COLUMN metal_prices.created_at IS 'Date et heure de l''insertion (date d''extraction).';


-- ============================================
-- Table: sync_logs (Logs des synchronisations)
-- ============================================
CREATE TABLE sync_logs (
    id SERIAL PRIMARY KEY,
    sync_type VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL,
    metals_updated INTEGER DEFAULT 0,
    error_message TEXT,
    duration_seconds DECIMAL(10, 3),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Contraintes
    CONSTRAINT check_sync_type CHECK (sync_type IN ('scheduled', 'manual', 'api')),
    CONSTRAINT check_status CHECK (status IN ('success', 'partial', 'failed')),
    CONSTRAINT check_metals_updated CHECK (metals_updated >= 0 AND metals_updated <= 3)
);

-- Index
CREATE INDEX idx_sync_logs_created_at ON sync_logs(created_at DESC);
CREATE INDEX idx_sync_logs_status ON sync_logs(status);