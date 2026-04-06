-- ============================================================
-- Portal Anticorrupción - Jefatura de Gabinete
-- Schema PostgreSQL
-- ============================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pg_trgm;  -- búsqueda por similitud de texto

-- ------------------------------------------------------------
-- NÓMINA DE PERSONAL
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS nomina (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    organismo       TEXT,
    unidad          TEXT,
    nombre          TEXT,
    apellido        TEXT,
    dni             VARCHAR(10),
    cuil            VARCHAR(13) UNIQUE,
    sexo            CHAR(1),
    cargo           TEXT,
    tipo_contratacion TEXT,
    agrupamiento    TEXT,
    nivel           TEXT,
    escalafon       TEXT,
    norma_designacion TEXT,
    fuente          TEXT DEFAULT 'MapaDelEstado',
    fecha_ingesta   TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_nomina_cuil    ON nomina(cuil);
CREATE INDEX IF NOT EXISTS idx_nomina_apellido ON nomina USING gin(apellido gin_trgm_ops);

-- ------------------------------------------------------------
-- CONTRATOS Y ADJUDICACIONES (COMPR.AR)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS contratos (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ejercicio           TEXT,
    nro_proceso         TEXT,
    organismo           TEXT,
    tipo_proceso        TEXT,
    objeto              TEXT,
    proveedor           TEXT,
    cuit_proveedor      VARCHAR(13),
    monto_adjudicado    NUMERIC(18,2),
    moneda              TEXT DEFAULT 'ARS',
    fecha_adjudicacion  DATE,
    fuente              TEXT DEFAULT 'COMPR.AR',
    fecha_ingesta       TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_contratos_cuit      ON contratos(cuit_proveedor);
CREATE INDEX IF NOT EXISTS idx_contratos_organismo ON contratos(organismo);
CREATE INDEX IF NOT EXISTS idx_contratos_fecha     ON contratos(fecha_adjudicacion);
CREATE INDEX IF NOT EXISTS idx_contratos_proveedor ON contratos USING gin(proveedor gin_trgm_ops);

-- ------------------------------------------------------------
-- PROVEEDORES (SIPRO)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS proveedores_sipro (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    cuit            VARCHAR(13) UNIQUE,
    razon_social    TEXT,
    tipo_persona    TEXT,
    estado          TEXT,
    rubro           TEXT,
    fecha_ingesta   TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sipro_cuit         ON proveedores_sipro(cuit);
CREATE INDEX IF NOT EXISTS idx_sipro_razon_social ON proveedores_sipro USING gin(razon_social gin_trgm_ops);

-- ------------------------------------------------------------
-- NORMAS BORA
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS normas_bora (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    numero_norma        TEXT UNIQUE,
    tipo_norma          TEXT,
    organismo           TEXT,
    fecha_publicacion   DATE,
    titulo              TEXT,
    url                 TEXT,
    termino_busqueda    TEXT,
    fuente              TEXT DEFAULT 'BORA',
    fecha_ingesta       TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bora_fecha    ON normas_bora(fecha_publicacion);
CREATE INDEX IF NOT EXISTS idx_bora_organismo ON normas_bora(organismo);

-- ------------------------------------------------------------
-- VÍNCULOS (resultado del motor de cruce)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS vinculos (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    cuil_a          VARCHAR(13) NOT NULL,   -- funcionario
    cuil_b          VARCHAR(13) NOT NULL,   -- pariente / proveedor
    tipo_vinculo    TEXT NOT NULL,          -- 'parentesco' | 'societario' | 'fondos'
    subtipo         TEXT,                   -- 'hermano' | 'socio' | 'desvio'
    nivel_alerta    TEXT DEFAULT 'media',   -- 'alta' | 'media' | 'baja'
    detalle         JSONB,
    fuente          TEXT,
    fecha_deteccion TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(cuil_a, cuil_b, tipo_vinculo, subtipo)
);

CREATE INDEX IF NOT EXISTS idx_vinculos_cuil_a ON vinculos(cuil_a);
CREATE INDEX IF NOT EXISTS idx_vinculos_cuil_b ON vinculos(cuil_b);
CREATE INDEX IF NOT EXISTS idx_vinculos_alerta ON vinculos(nivel_alerta);
CREATE INDEX IF NOT EXISTS idx_vinculos_tipo   ON vinculos(tipo_vinculo);

-- ------------------------------------------------------------
-- ALERTAS GENERADAS
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS alertas (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tipo            TEXT NOT NULL,       -- 'nepotismo' | 'capitalismo_amigos' | 'sobreprecio'
    nivel           TEXT NOT NULL,       -- 'alta' | 'media' | 'baja'
    titulo          TEXT NOT NULL,
    descripcion     TEXT,
    funcionario_cuil VARCHAR(13),
    proveedor_cuit  VARCHAR(13),
    contrato_id     UUID REFERENCES contratos(id),
    vinculo_id      UUID REFERENCES vinculos(id),
    monto_involucrado NUMERIC(18,2),
    resuelta        BOOLEAN DEFAULT FALSE,
    fecha_creacion  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alertas_nivel    ON alertas(nivel);
CREATE INDEX IF NOT EXISTS idx_alertas_tipo     ON alertas(tipo);
CREATE INDEX IF NOT EXISTS idx_alertas_resuelta ON alertas(resuelta);

-- ------------------------------------------------------------
-- VISTA: dashboard principal
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW v_dashboard AS
SELECT
    a.id,
    a.tipo,
    a.nivel,
    a.titulo,
    a.descripcion,
    a.monto_involucrado,
    a.fecha_creacion,
    n.nombre || ' ' || n.apellido AS funcionario_nombre,
    n.cargo                        AS funcionario_cargo,
    c.proveedor                    AS proveedor_nombre,
    c.monto_adjudicado             AS monto_contrato
FROM alertas a
LEFT JOIN nomina   n ON n.cuil = a.funcionario_cuil
LEFT JOIN contratos c ON c.id  = a.contrato_id
WHERE a.resuelta = FALSE
ORDER BY
    CASE a.nivel WHEN 'alta' THEN 1 WHEN 'media' THEN 2 ELSE 3 END,
    a.fecha_creacion DESC;