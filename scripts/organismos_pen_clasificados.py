"""
organismos_pen_clasificados.py
==============================
Clasificación del PEN según DNU 8/2023 (Milei, 10/12/2023).

Estructura exacta según artículos del decreto:
  Art. 5  → Ciencia y Tecnología → JGM
  Art. 6  → Asuntos Estratégicos → JGM
  Art. 7  → Ambiente + Turismo y Deportes → Interior
  Art. 8  → Transporte + Obras Públicas + Desarrollo Territorial → Infraestructura
  Art. 9  → Justicia y DDHH → Justicia
  Art. 10 → Educación + Cultura + Trabajo + Desarrollo Social + Mujeres → Capital Humano
  Art. 13 → UIF → Justicia
  Art. 16 → ANMaC → Seguridad
  Art. 17 → ANDIS → JGM
  Art. 18 → INAES → Capital Humano
  Art. 19 → Agricultura Familiar → Capital Humano

Secretarías Presidenciales (Art. 9 Ley de Ministerios):
  1. General (Karina Milei) → SGP
  2. Legal y Técnica → Presidencia
  3. Comunicación y Prensa → Presidencia
"""

AREAS = {

    # ─────────────────────────────────────────────────────────────────
    # PRESIDENCIA DE LA NACIÓN
    # Art. 9 — Secretarías que asisten directamente al Presidente
    # ─────────────────────────────────────────────────────────────────
    "presidencia": {
        "label": "Presidencia de la Nación",
        "organismos": {
            "586":  "Secretaría Legal y Técnica",
            "1771": "Secretaría de Comunicación y Medios",
            "1726": "SEDRONAR",
            "1346": "Procuración del Tesoro de la Nación",
        }
    },

    # ─────────────────────────────────────────────────────────────────
    # SECRETARÍA GENERAL DE LA PRESIDENCIA
    # ─────────────────────────────────────────────────────────────────
    "sgp": {
        "label": "Secretaría General de la Presidencia",
        "organismos": {
            "588":  "Secretaría General de la Presidencia de la Nación",
        }
    },

    # ─────────────────────────────────────────────────────────────────
    # JEFATURA DE GABINETE DE MINISTROS
    # Art. 5: absorbió Ciencia y Tecnología
    # Art. 6: absorbió Asuntos Estratégicos
    # Art. 17: ANDIS pasa a JGM
    # ─────────────────────────────────────────────────────────────────
    "jgm": {
        "label": "Jefatura de Gabinete de Ministros",
        "organismos": {
            "591":  "Jefatura de Gabinete de Ministros",
            "1742": "Secretaría de Innovación Pública",
            "1926": "Secretaría de Innovación, Ciencia y Tecnología",
            "1938": "Ministerio de Desregulación y Transformación del Estado",
            "619":  "Sindicatura General de la Nación (SIGEN)",
            "593":  "Agencia de Administración de Bienes del Estado (AABE)",
            "1354": "Agencia de Acceso a la Información Pública (AAIP)",
            "903":  "Agencia Nacional de Discapacidad (ANDIS)",  # Art. 17
            # Ciencia y Tecnología (Art. 5)
            "1051": "CONICET",
            "933":  "Comisión Nacional de Energía Atómica (CNEA)",
            "1316": "Comisión Nacional de Actividades Espaciales (CONAE)",
            "942":  "Instituto Nacional del Agua (INA)",
            "675":  "Autoridad Regulatoria Nuclear (ARN)",
            "1829": "Agencia Nacional de Promoción de la Investigación y Desarrollo (AGENCIA I+D)",
            "912":  "Fundación Miguel Lillo",
            "710":  "Instituto de Investigaciones Científicas para la Defensa (CITEDEF)",
        }
    },

    # ─────────────────────────────────────────────────────────────────
    # MINISTERIO DEL INTERIOR
    # Art. 7: absorbió Ambiente y Turismo y Deportes
    # ─────────────────────────────────────────────────────────────────
    "interior": {
        "label": "Ministerio del Interior",
        "organismos": {
            "1732": "Ministerio del Interior",
            "703":  "Registro Nacional de las Personas (RENAPER)",
            "697":  "Dirección Nacional de Migraciones",
            "1725": "INADI",
            # Ambiente (Art. 7)
            "1730": "Ex Ministerio de Ambiente y Desarrollo Sostenible",
            "910":  "Administración de Parques Nacionales",
            "801":  "Autoridad de Cuenca Matanza Riachuelo (ACUMAR)",
            # Turismo y Deportes (Art. 7)
            "1731": "Ex Ministerio de Turismo y Deportes",
            "1376": "Agencia de Deportes Nacional",
        }
    },

    # ─────────────────────────────────────────────────────────────────
    # MINISTERIO DE RELACIONES EXTERIORES
    # ─────────────────────────────────────────────────────────────────
    "exteriores": {
        "label": "Ministerio de Relaciones Exteriores, Comercio Internacional y Culto",
        "organismos": {
            "1727": "Ministerio de Relaciones Exteriores, Comercio Internacional y Culto",
        }
    },

    # ─────────────────────────────────────────────────────────────────
    # MINISTERIO DE ECONOMÍA
    # Absorbe: Agricultura, Producción, Energía, Minería (Art. 20 del decreto)
    # ─────────────────────────────────────────────────────────────────
    "economia": {
        "label": "Ministerio de Economía",
        "organismos": {
            "1739": "Ministerio de Economía",
            "663":  "INDEC",
            "679":  "Comisión Nacional de Comercio Exterior",
            "693":  "Comisión Nacional de Valores (CNV)",
            "726":  "Superintendencia de Seguros de la Nación",
            "687":  "Tribunal Fiscal de la Nación",
            "752":  "Tribunal de Tasaciones de la Nación",
            "944":  "Ministerio de Finanzas",
            "1378": "BICE Fideicomisos S.A.",
            # Agricultura, Ganadería y Pesca
            "1741": "Ex Ministerio de Agricultura, Ganadería y Pesca",
            "718":  "SENASA",
            "899":  "INIDEP",
            "1047": "Instituto Nacional de Vitivinicultura (INV)",
            "1744": "Instituto Nacional de Semillas (INASE)",
            "1898": "INTA",
            # Producción e Industria
            "1740": "Ex Ministerio de Desarrollo Productivo",
            "705":  "INTI",
            "916":  "INPI",
            # Energía y Minería
            "1733": "Secretaría de Energía",
            "1265": "ENARGAS",
            "1318": "ENRE",
            "1571": "Consejo Federal de la Energía Eléctrica",
            "1982": "Ente Nacional Regulador del Gas y la Electricidad",
            "937":  "SEGEMAR",
        }
    },

    # ─────────────────────────────────────────────────────────────────
    # MINISTERIO DE INFRAESTRUCTURA
    # Art. 8: absorbió Transporte + Obras Públicas + Desarrollo Territorial
    # Art. 21: también incluye Comunicaciones
    # ─────────────────────────────────────────────────────────────────
    "infraestructura": {
        "label": "Ministerio de Infraestructura",
        "organismos": {
            # Transporte (Art. 8)
            "645":  "Ex Ministerio de Transporte",
            "708":  "Dirección Nacional de Vialidad (DNV)",
            "742":  "Administración Nacional de Aviación Civil (ANAC)",
            "712":  "Comisión Nacional de Regulación del Transporte (CNRT)",
            "796":  "Organismo Regulador del Sistema Nacional de Aeropuertos (ORSNA)",
            "794":  "Junta de Seguridad en el Transporte",
            "738":  "Organismo Regulador de Seguridad de Presas",
            "720":  "ENOHSA",
            "1340": "Operadora Ferroviaria Sociedad del Estado (SOFSE)",
            "1955": "Agencia Nacional de Puertos y Navegación",
            # Obras Públicas (Art. 8)
            "1756": "Ex Ministerio de Obras Públicas",
            "1907": "Infraestructura Económica y Social",
            # Desarrollo Territorial y Hábitat (Art. 8)
            "1757": "Ex Ministerio de Desarrollo Territorial y Hábitat",
            # Comunicaciones (Art. 21)
            "611":  "Ex Ministerio de Comunicaciones",
            "683":  "ENACOM",
            "1737": "Secretaría de Medios y Comunicación Pública",
            "1508": "Defensoría del Público de Servicios de Comunicación Audiovisual",
        }
    },

    # ─────────────────────────────────────────────────────────────────
    # MINISTERIO DE JUSTICIA
    # Art. 9: absorbió Justicia y DDHH
    # Art. 13: UIF pasa a Justicia
    # ─────────────────────────────────────────────────────────────────
    "justicia": {
        "label": "Ministerio de Justicia",
        "organismos": {
            "599":  "Ministerio de Justicia y Derechos Humanos",
            "792":  "Centro Internacional para la Promoción de los Derechos Humanos",
            "914":  "Agencia Nacional de Seguridad Vial (ANSV)",
            "691":  "Unidad de Información Financiera (UIF)",  # Art. 13
        }
    },

    # ─────────────────────────────────────────────────────────────────
    # MINISTERIO DE SEGURIDAD
    # Art. 16: ANMaC bajo Seguridad
    # ─────────────────────────────────────────────────────────────────
    "seguridad": {
        "label": "Ministerio de Seguridad",
        "organismos": {
            "637":  "Ministerio de Seguridad",
            "1129": "Gendarmería Nacional",
            "1081": "Prefectura Naval Argentina",
            "695":  "Policía de Seguridad Aeroportuaria (PSA)",
            "659":  "Servicio Penitenciario Federal (SPF)",
            "701":  "Agencia Nacional de Materiales Controlados (ANMaC)",  # Art. 16
            "1263": "Superintendencia de la Policía Federal",
            "1127": "Caja de Retiros Jubilaciones Policía Federal",
            "1344": "Superintendencia de Bienestar de la Policía Federal",
            "1832": "Ente de Cooperación Técnica Servicio Penitenciario Federal",
            "1475": "Dirección de Obra Social del Servicio Penitenciario Federal",
        }
    },

    # ─────────────────────────────────────────────────────────────────
    # MINISTERIO DE DEFENSA
    # ─────────────────────────────────────────────────────────────────
    "defensa": {
        "label": "Ministerio de Defensa",
        "organismos": {
            "647":  "Ministerio de Defensa",
            "1049": "Estado Mayor Conjunto (EMCO)",
            "939":  "Estado Mayor General del Ejército",
            "949":  "Estado Mayor General de la Armada",
            "808":  "Estado Mayor General de la Fuerza Aérea",
            "1743": "Subsecretaría de Planeamiento Operativo y Logística de la Defensa",
            "728":  "Dirección General de Fabricaciones Militares (DGFM)",
            "724":  "Instituto Geográfico Nacional (IGN)",
            "722":  "Servicio Meteorológico Nacional (SMN)",
            "1104": "Instituto de Ayuda Financiera para Retiros y Pensiones Militares",
            "1693": "Instituto de Obra Social de las Fuerzas Armadas (IOSFA)",
            "1500": "Universidad de la Defensa Nacional (UNDEF)",
        }
    },

    # ─────────────────────────────────────────────────────────────────
    # MINISTERIO DE SALUD
    # ─────────────────────────────────────────────────────────────────
    "salud": {
        "label": "Ministerio de Salud",
        "organismos": {
            "1728": "Ministerio de Salud",
            "1280": "ANMAT",
            "962":  "ANLIS Dr. Carlos Malbrán",
            "935":  "Hospital Nacional Salud Mental Laura Bonaparte",
            "1311": "Hospital Nacional Dr. Baldomero Sommer",
            "1746": "Hospital Nacional Prof. Alejandro Posadas",
            "968":  "Colonia Nacional Dr. Manuel Montes de Oca",
            "1747": "Instituto Nacional de Rehabilitación Psicofísica del Sur",
            "803":  "Servicio Nacional de Rehabilitación",
            "1748": "Superintendencia de Servicios de Salud",
            "790":  "Instituto Nacional del Cáncer",
            "784":  "Agencia Nacional de Laboratorios Públicos (ANLAP)",
            "1256": "INCUCAI",
            "1977": "Administración Nacional de Establecimientos de Salud",
        }
    },

    # ─────────────────────────────────────────────────────────────────
    # MINISTERIO DE CAPITAL HUMANO
    # Art. 10: absorbió Educación + Cultura + Trabajo + Desarrollo Social + Mujeres
    # Art. 18: INAES → Capital Humano
    # Art. 19: Agricultura Familiar → Capital Humano
    # ─────────────────────────────────────────────────────────────────
    "capital_humano": {
        "label": "Ministerio de Capital Humano",
        "organismos": {
            "1906": "Ministerio de Capital Humano",
            "1925": "Desarrollo Humano y Economía Solidaria",
            # Educación (Art. 10)
            "1734": "Secretaría de Educación",
            "744":  "CONEAU",
            # Cultura (Art. 10)
            "1736": "Secretaría de Cultura",
            "805":  "Teatro Nacional Cervantes",
            "1102": "Biblioteca Nacional",
            "798":  "Instituto Nacional del Teatro",
            "905":  "Fondo Nacional de las Artes",
            "1496": "INCAA",
            "665":  "Instituto Nacional de Asuntos Indígenas (INAI)",
            # Trabajo (Art. 10)
            "1738": "Ex Ministerio de Trabajo, Empleo y Seguridad Social",
            "736":  "ANSES",
            "740":  "Secretaría Nacional de Niñez, Adolescencia y Familia (SENAF)",
            "661":  "Consejo Nacional de Coordinación de Políticas Sociales",
            "1745": "Superintendencia de Riesgos del Trabajo (SRT)",
            "1383": "INSSJP (PAMI)",
            # Mujeres (Art. 10)
            "908":  "Instituto Nacional de las Mujeres",
            "1754": "Ex Ministerio de las Mujeres, Géneros y Diversidad",
            # INAES (Art. 18)
            "734":  "Instituto Nacional de Asociativismo y Economía Social (INAES)",
        }
    },

    # ─────────────────────────────────────────────────────────────────
    # ORGANISMOS DE CONTROL INDEPENDIENTES
    # ─────────────────────────────────────────────────────────────────
    "control": {
        "label": "Organismos de Control",
        "organismos": {
            "1764": "Auditoría General de la Nación (AGN)",
            "1425": "Banco Nacional de Datos Genéticos",
        }
    },

}

# ─────────────────────────────────────────────────────────────────────────────
# FLAT: {saf: {nombre, area, area_label}} — primera aparición gana
# ─────────────────────────────────────────────────────────────────────────────
TODOS_LOS_SAF = {}
for area_key, area_data in AREAS.items():
    for saf, nombre in area_data["organismos"].items():
        if saf not in TODOS_LOS_SAF:
            TODOS_LOS_SAF[saf] = {
                "nombre":      nombre,
                "area":        area_key,
                "area_label":  area_data["label"],
            }

if __name__ == "__main__":
    total_unicos = len(TODOS_LOS_SAF)
    print(f"Total SAFs únicos clasificados: {total_unicos}")
    print(f"Total áreas: {len(AREAS)}\n")
    for area_key, area_data in AREAS.items():
        safs = list(area_data["organismos"].keys())
        print(f"  [{area_key}] {area_data['label']}: {len(safs)} organismos")
    print(f"\nSAFs: {sorted(TODOS_LOS_SAF.keys(), key=lambda x: int(x))}")