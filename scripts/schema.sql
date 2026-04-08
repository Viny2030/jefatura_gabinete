-- ============================================================
-- schema.sql — jgm_anticorrupcion
-- ============================================================

-- Tabla principal de contratos (datos de listado comprar.gob.ar)
CREATE TABLE IF NOT EXISTS contratos (
    id                  SERIAL PRIMARY KEY,
    numero_proceso      VARCHAR(60)  NOT NULL,
    nombre_proceso      TEXT,
    tipo_proceso        VARCHAR(80),
    fecha_apertura      DATE,
    estado              VARCHAR(60),
    unidad_ejecutora    TEXT,
    saf_texto           TEXT,          -- "374 - Estado Mayor General del Ejercito"
    area                VARCHAR(40),   -- clave interna: jgm, economia, salud, etc.
    saf                 VARCHAR(10),   -- ID numérico del SAF
    organismo           TEXT,          -- nombre completo del organismo scrapeado

    -- Campos de detalle (se completan con scraper_detalle.py)
    monto_adjudicado    NUMERIC(20,2),
    moneda              VARCHAR(10),
    proveedor_razon     TEXT,
    proveedor_cuit      VARCHAR(20),

    -- Metadata
    url_detalle         TEXT,
    detalle_scrapeado   BOOLEAN DEFAULT FALSE,
    created_at          TIMESTAMP DEFAULT NOW(),
    updated_at          TIMESTAMP DEFAULT NOW(),

    -- Evitar duplicados
    UNIQUE(numero_proceso, saf)
);

-- Índices para consultas frecuentes
CREATE INDEX IF NOT EXISTS idx_contratos_area       ON contratos(area);
CREATE INDEX IF NOT EXISTS idx_contratos_saf        ON contratos(saf);
CREATE INDEX IF NOT EXISTS idx_contratos_fecha      ON contratos(fecha_apertura);
CREATE INDEX IF NOT EXISTS idx_contratos_estado     ON contratos(estado);
CREATE INDEX IF NOT EXISTS idx_contratos_cuit       ON contratos(proveedor_cuit);
CREATE INDEX IF NOT EXISTS idx_contratos_detalle    ON contratos(detalle_scrapeado);

-- Tabla de proveedores (se construye al scrapear detalles)
CREATE TABLE IF NOT EXISTS proveedores (
    id              SERIAL PRIMARY KEY,
    cuit            VARCHAR(20) UNIQUE NOT NULL,
    razon_social    TEXT,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_proveedores_cuit ON proveedores(cuit);

-- Vista útil: contratos con detalle completo
CREATE OR REPLACE VIEW v_contratos_completos AS
SELECT
    c.id,
    c.numero_proceso,
    c.nombre_proceso,
    c.tipo_proceso,
    c.fecha_apertura,
    c.estado,
    c.area,
    c.organismo,
    c.monto_adjudicado,
    c.moneda,
    c.proveedor_cuit,
    p.razon_social AS proveedor_razon_social,
    c.detalle_scrapeado
FROM contratos c
LEFT JOIN proveedores p ON p.cuit = c.proveedor_cuit;

-- Vista resumen por área
CREATE OR REPLACE VIEW v_resumen_area AS
SELECT
    area,
    COUNT(*)                                        AS total_contratos,
    COUNT(*) FILTER (WHERE detalle_scrapeado)       AS con_detalle,
    SUM(monto_adjudicado)                           AS monto_total,
    COUNT(DISTINCT proveedor_cuit)                  AS proveedores_unicos,
    MIN(fecha_apertura)                             AS fecha_min,
    MAX(fecha_apertura)                             AS fecha_max
FROM contratos
GROUP BY area
ORDER BY total_contratos DESC;