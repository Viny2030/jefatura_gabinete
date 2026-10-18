"""
chat_v2.py — Agente IA v2 (solo lectura) para jgm.html / sgp.html / presidencia.html
====================================================================================

Convive con el agente v1 de api_server.py (no lo reemplaza ni lo modifica).
Se monta desde src/api/server_v2.py en:

  POST /api/v2/chat/{area}         → mismo contrato que /api/v1/chat/{area}
  GET  /api/v2/chat/diagnostico    → cobertura de datos por área (sin secretos)

Qué corrige respecto de v1 (ver docs/REFORMAS_P0.md):
  1. Búsqueda normalizada: sin tildes, sin mayúsculas, "S.A." == "SA", y los
     sufijos societarios (SA, SRL, SAS, INC…) no son obligatorios.
     v1: "EJES SA" no encontraba "EJES S.A.".
  2. Busca en proveedor + objeto + CUIT + número de proceso + organismo.
     v1 solo miraba `proveedor`, y en ~70 % de los contratos ese campo tiene el
     título del proceso (COMPR.AR no publicó razón social), así que "Microsoft"
     no aparecía en SGP aunque estuviera en el objeto.
  3. Resultados compactos con totales (total_coincidencias, con_monto…).
     v1 cortaba el JSON en 4000 caracteres: de 15 registros llegaban ~7 y el
     JSON roto; los "rankings" salían de esa muestra truncada.
  4. Herramientas de agregación (ranking_proveedores, resumen_area) para que
     el modelo no ordene a mano una muestra parcial, y que informen cobertura.
  5. Alcance: área, jurisdicción ampliada (JGM) o todo el PEN (por_area/*).
  6. IP real detrás del proxy de Railway (X-Forwarded-For) para el rate limit.
  7. Respuestas en texto plano: el chat pinta con textContent, el markdown se
     veía crudo (tablas con "|", "**").
"""

from __future__ import annotations

import difflib
import json
import os
import re
import unicodedata
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import httpx
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

# ── Config ────────────────────────────────────────────────────────────────────
_SRC = Path(__file__).resolve().parent.parent
DATA_DIRS = [_SRC / "data", _SRC / "frontend" / "data"]

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-5-5")
CHAT_RATE_LIMIT_DIA = int(os.getenv("CHAT_RATE_LIMIT_DIARIO", "30"))
MAX_TOKENS = int(os.getenv("CHAT_V2_MAX_TOKENS", "1200"))
MAX_RONDAS = 6                # idas y vueltas de tool-use por mensaje
MAX_CHARS_TOOL = 6000         # tope por resultado de tool (se respeta armando, no cortando)

AREAS_VALIDAS = {"jgm", "sgp", "presidencia"}
AREA_LABELS = {
    "jgm": "Jefatura de Gabinete de Ministros",
    "sgp": "Secretaría General de la Presidencia",
    "presidencia": "Presidencia de la Nación",
}
ALCANCES = ("area", "ampliada", "todo_pen")

router = APIRouter(tags=["Agente IA v2"])


# ── Carga de datos (con caché por mtime) ──────────────────────────────────────
_cache: dict[str, tuple[float, object]] = {}


def cargar(nombre: str):
    """Lee un JSON publicado (src/data o src/frontend/data). [] si no existe."""
    for base in DATA_DIRS:
        path = base / nombre
        if path.exists():
            mtime = path.stat().st_mtime
            hit = _cache.get(str(path))
            if hit and hit[0] == mtime:
                return hit[1]
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            _cache[str(path)] = (mtime, data)
            return data
    return []


def _lista(data) -> list[dict]:
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        for k in ("contratos", "cruces", "personal", "data"):
            if isinstance(data.get(k), list):
                return [x for x in data[k] if isinstance(x, dict)]
    return []


def contratos_de(area: str, alcance: str = "area") -> list[dict]:
    """Contratos según alcance. Deduplica por número de proceso."""
    nombres = [f"contratos_{area}.json"]
    if alcance == "ampliada" and area == "jgm":
        nombres.append("contratos_jgm_ampliada.json")
    if alcance == "todo_pen":
        nombres += [f"contratos_{a}.json" for a in AREAS_VALIDAS]
        nombres.append("contratos_jgm_ampliada.json")
        carpeta = next((b / "por_area" for b in DATA_DIRS if (b / "por_area").is_dir()), None)
        if carpeta:
            nombres += [f"por_area/{p.name}" for p in sorted(carpeta.glob("contratos_*.json"))]
    vistos, out = set(), []
    for n in nombres:
        for c in _lista(cargar(n)):
            clave = c.get("numero_proceso") or id(c)
            if clave in vistos:
                continue
            vistos.add(clave)
            out.append(c)
    return out


# ── Normalización y coincidencia ──────────────────────────────────────────────
_SUFIJOS = {
    "sa", "srl", "sas", "sau", "saic", "saci", "sacif", "sca", "sh", "se",
    "inc", "ltd", "ltda", "llc", "cia", "coop", "ltdo", "y", "de", "del",
    "la", "el", "los", "las", "s", "a", "l", "r", "c", "i", "f",
}


def normalizar(texto) -> str:
    """minúsculas, sin tildes, 'S.A.' → 'sa', resto de puntuación → espacio."""
    if texto is None:
        return ""
    t = unicodedata.normalize("NFD", str(texto))
    t = "".join(ch for ch in t if not unicodedata.combining(ch)).lower()
    t = t.replace(".", "")                     # s.a. → sa, s.r.l. → srl
    t = re.sub(r"[^a-z0-9]+", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def tokens_requeridos(consulta: str) -> list[str]:
    toks = normalizar(consulta).split()
    req = [t for t in toks if t not in _SUFIJOS]
    return req or toks


def solo_digitos(texto) -> str:
    return re.sub(r"\D", "", str(texto or ""))


def coincide(consulta: str, *campos) -> bool:
    """Todos los tokens significativos de la consulta aparecen en los campos."""
    req = tokens_requeridos(consulta)
    if not req:
        return True
    texto = " " + " ".join(normalizar(c) for c in campos if c) + " "
    return all(f" {t}" in texto for t in req)   # prefijo de palabra


def tiene_razon_social(c: dict) -> bool:
    """False cuando `proveedor` es en realidad el título del proceso."""
    prov = str(c.get("proveedor") or "").strip()
    obj = str(c.get("objeto") or "").strip()
    if not prov:
        return False
    if str(c.get("cuit") or "").strip():
        return True
    return not (obj and obj.startswith(prov[:40]))


def _monto(c: dict) -> float:
    try:
        v = float(c.get("monto_adjudicado") or 0)
        return v if v == v else 0.0    # NaN → 0
    except (TypeError, ValueError):
        return 0.0


def compacto(c: dict) -> dict:
    razon = tiene_razon_social(c)
    return {
        "numero_proceso": c.get("numero_proceso"),
        "proveedor": c.get("proveedor") if razon else None,
        "cuit": c.get("cuit") or None,
        "monto": _monto(c) or None,
        "moneda": c.get("moneda") or ("ARS" if _monto(c) else None),
        "fecha": c.get("fecha_adjudicacion") or c.get("fecha_apertura") or None,
        "estado": c.get("estado") or None,
        "tipo": c.get("tipo_contratacion") or None,
        "objeto": str(c.get("objeto") or "")[:140] or None,
        "organismo": str(c.get("organismo") or "")[:70] or None,
    }


def empaquetar(items: list[dict], total: int, extra: dict | None = None,
               max_chars: int = MAX_CHARS_TOOL) -> dict:
    """Arma la respuesta de la tool sin pasarse de max_chars y sin cortar JSON."""
    out = {"total_coincidencias": total, "mostrando": 0, "resultados": []}
    if extra:
        out.update(extra)
    for it in items:
        out["resultados"].append(it)
        out["mostrando"] = len(out["resultados"])
        if len(json.dumps(out, ensure_ascii=False, default=str)) > max_chars:
            out["resultados"].pop()
            out["mostrando"] = len(out["resultados"])
            out["truncado"] = True
            break
    return out


# ── Tools ─────────────────────────────────────────────────────────────────────
def tool_buscar_contratos(area: str, texto: str = None, organismo: str = None,
                          cuit: str = None, monto_min: float = None,
                          alcance: str = "area", ordenar_por: str = "monto",
                          limite: int = 15) -> dict:
    alcance = alcance if alcance in ALCANCES else "area"
    data = contratos_de(area, alcance)
    out = data
    if texto:
        dig = solo_digitos(texto)
        if len(dig) == 11:
            out = [c for c in out if solo_digitos(c.get("cuit")) == dig]
        else:
            out = [c for c in out if coincide(texto, c.get("proveedor"), c.get("objeto"),
                                              c.get("numero_proceso"), c.get("organismo"))]
    if cuit:
        dig = solo_digitos(cuit)
        out = [c for c in out if solo_digitos(c.get("cuit")) == dig]
    if organismo:
        out = [c for c in out if coincide(organismo, c.get("organismo"), c.get("organismo_saf"))]
    if monto_min:
        out = [c for c in out if _monto(c) >= float(monto_min)]
    if ordenar_por == "fecha":
        out = sorted(out, key=lambda c: str(c.get("fecha_adjudicacion") or ""), reverse=True)
    else:
        out = sorted(out, key=_monto, reverse=True)

    extra = {
        "alcance": alcance,
        "con_monto": sum(1 for c in out if _monto(c)),
        "con_razon_social": sum(1 for c in out if tiene_razon_social(c)),
    }
    if texto and not out:
        extra["sugerencias"] = sugerencias(texto, data)
    limite = max(1, min(int(limite or 15), 40))
    return empaquetar([compacto(c) for c in out[:limite]], len(out), extra)


def sugerencias(texto: str, data: list[dict], n: int = 5) -> list[str]:
    """Nombres de proveedor parecidos (errores de tipeo) cuando no hay match."""
    nombres = {normalizar(c.get("proveedor")): c.get("proveedor")
               for c in data if tiene_razon_social(c)}
    q = " ".join(tokens_requeridos(texto))
    parecidos = difflib.get_close_matches(q, list(nombres), n=n, cutoff=0.6)
    if not parecidos:   # probar contra cada palabra del nombre
        por_token = defaultdict(list)
        for norm in nombres:
            for tok in norm.split():
                por_token[tok].append(norm)
        for tok in difflib.get_close_matches(q, list(por_token), n=n, cutoff=0.75):
            parecidos += por_token[tok]
    return [nombres[p] for p in dict.fromkeys(parecidos)][:n]


def tool_ranking_proveedores(area: str, alcance: str = "area", moneda: str = "ARS",
                             top: int = 10) -> dict:
    alcance = alcance if alcance in ALCANCES else "area"
    data = contratos_de(area, alcance)
    moneda = (moneda or "ARS").upper()
    agg: dict[str, dict] = {}
    for c in data:
        m = _monto(c)
        if not m or not tiene_razon_social(c):
            continue
        mon = (c.get("moneda") or "ARS").upper()
        if mon != moneda:
            continue
        clave = solo_digitos(c.get("cuit")) or normalizar(c.get("proveedor"))
        a = agg.setdefault(clave, {"proveedor": c.get("proveedor"), "cuit": c.get("cuit") or None,
                                   "monto_total": 0.0, "contratos": 0})
        a["monto_total"] += m
        a["contratos"] += 1
    ranking = sorted(agg.values(), key=lambda a: a["monto_total"], reverse=True)
    top = max(1, min(int(top or 10), 30))
    return empaquetar(ranking[:top], len(ranking), {
        "alcance": alcance, "moneda": moneda,
        "contratos_universo": len(data),
        "contratos_con_monto_y_proveedor": sum(a["contratos"] for a in ranking),
        "advertencia": ("El ranking solo cubre contratos con monto y razón social publicados "
                        "en COMPR.AR; el resto no tiene esos datos y no puede ordenarse."),
    })


def tool_resumen_area(area: str, alcance: str = "area") -> dict:
    alcance = alcance if alcance in ALCANCES else "area"
    data = contratos_de(area, alcance)
    por_moneda = defaultdict(float)
    por_estado = defaultdict(int)
    por_anio = defaultdict(int)
    for c in data:
        if _monto(c):
            por_moneda[(c.get("moneda") or "ARS").upper()] += _monto(c)
        por_estado[c.get("estado") or "sin estado"] += 1
        por_anio[str(c.get("ejercicio") or str(c.get("fecha_adjudicacion") or "")[:4] or "s/d")] += 1
    n = len(data)
    con_monto = sum(1 for c in data if _monto(c))
    con_razon = sum(1 for c in data if tiene_razon_social(c))
    return {
        "area": AREA_LABELS.get(area, area), "alcance": alcance,
        "contratos": n,
        "con_monto": con_monto,
        "con_razon_social": con_razon,
        "cobertura_monto_pct": round(100 * con_monto / n, 1) if n else 0,
        "cobertura_razon_social_pct": round(100 * con_razon / n, 1) if n else 0,
        "monto_por_moneda": {k: round(v, 2) for k, v in por_moneda.items()},
        "por_estado": dict(sorted(por_estado.items(), key=lambda x: -x[1])[:8]),
        "por_anio": dict(sorted(por_anio.items())),
        "personal": len(_lista(cargar(f"personal_{area}.json"))),
    }


def tool_buscar_personal(area: str, texto: str = None, cuil: str = None,
                         limite: int = 15) -> dict:
    data = _lista(cargar(f"personal_{area}.json"))
    out = data
    if texto:
        out = [p for p in out if coincide(texto, p.get("apellido"), p.get("nombre"),
                                          p.get("cargo"), p.get("sub_organismo"),
                                          p.get("organismo"))]
    if cuil:
        dig = solo_digitos(str(cuil).split(".")[0])
        out = [p for p in out if solo_digitos(str(p.get("cuil") or "").split(".")[0]) == dig]
    campos = ("apellido", "nombre", "cargo", "jerarquia", "organismo", "sub_organismo",
              "fecha_designacion", "norma_designacion", "jgm_al_ingreso")
    items = [{k: p.get(k) for k in campos if p.get(k) not in (None, "")} for p in out]
    limite = max(1, min(int(limite or 15), 40))
    return empaquetar(items[:limite], len(out))


def tool_buscar_cruces(area: str, texto: str = None, solo_esta_area: bool = False,
                       nivel: str = None, limite: int = 15) -> dict:
    out = _lista(cargar("cruces.json"))
    if solo_esta_area:
        out = [c for c in out if (c.get("area") or "") == area]
    if nivel:
        out = [c for c in out if str(c.get("nivel", "")).upper().startswith(nivel.upper()[:4])]
    if texto:
        dig = solo_digitos(texto)
        if len(dig) == 11:
            out = [c for c in out if solo_digitos(c.get("cuit")) == dig]
        else:
            out = [c for c in out if coincide(texto, c.get("funcionario"), c.get("empresa"),
                                              c.get("organismo"), c.get("apellido"))]
    campos = ("nivel", "area", "tipo", "funcionario", "cargo", "empresa", "cuit",
              "organismo", "monto", "contratos", "link")
    items = [{k: c.get(k) for k in campos if c.get(k) not in (None, "")} for c in out]
    limite = max(1, min(int(limite or 15), 40))
    return empaquetar(items[:limite], len(out), {
        "nota": "Indicador algorítmico de riesgo (coincidencia de CUIL/apellido), no una acusación."})


_ALCANCE_PROP = {"type": "string", "enum": list(ALCANCES),
                 "description": "area = solo esta área (default); ampliada = JGM + organismos con SAF propio "
                                "(CONICET, CONAE, ENACOM…); todo_pen = todos los contratos del monitor"}

TOOLS_SCHEMA = [
    {
        "name": "buscar_contratos",
        "description": ("Busca contratos por texto libre (nombre de empresa, palabra del objeto, número "
                        "de proceso o CUIT), organismo y/o monto mínimo. No distingue tildes ni "
                        "mayúsculas y 'S.A.' equivale a 'SA'. Devuelve total_coincidencias y una muestra."),
        "input_schema": {"type": "object", "properties": {
            "texto": {"type": "string", "description": "Empresa, palabra clave, NUP o CUIT"},
            "organismo": {"type": "string"},
            "cuit": {"type": "string"},
            "monto_min": {"type": "number", "description": "Monto mínimo adjudicado"},
            "alcance": _ALCANCE_PROP,
            "ordenar_por": {"type": "string", "enum": ["monto", "fecha"]},
            "limite": {"type": "integer", "minimum": 1, "maximum": 40},
        }},
    },
    {
        "name": "ranking_proveedores",
        "description": ("Ranking de proveedores por monto total adjudicado (agrega todos los contratos "
                        "del alcance, no una muestra). Usala para 'mayores proveedores', 'top N', etc."),
        "input_schema": {"type": "object", "properties": {
            "alcance": _ALCANCE_PROP,
            "moneda": {"type": "string", "enum": ["ARS", "USD", "EUR"]},
            "top": {"type": "integer", "minimum": 1, "maximum": 30},
        }},
    },
    {
        "name": "resumen_area",
        "description": ("Totales del área: cantidad de contratos, cuántos tienen monto y razón social "
                        "publicados (cobertura), montos por moneda, por estado y por año, y personal."),
        "input_schema": {"type": "object", "properties": {"alcance": _ALCANCE_PROP}},
    },
    {
        "name": "buscar_personal",
        "description": "Busca funcionarios del área por apellido, nombre, cargo o dependencia, o por CUIL.",
        "input_schema": {"type": "object", "properties": {
            "texto": {"type": "string"},
            "cuil": {"type": "string"},
            "limite": {"type": "integer", "minimum": 1, "maximum": 40},
        }},
    },
    {
        "name": "buscar_cruces",
        "description": ("Busca alertas de cruce funcionario↔proveedor en todo el monitor por nombre de "
                        "funcionario, empresa, organismo o CUIT; opcionalmente solo de esta área o por nivel."),
        "input_schema": {"type": "object", "properties": {
            "texto": {"type": "string"},
            "solo_esta_area": {"type": "boolean"},
            "nivel": {"type": "string", "enum": ["ALTO", "MEDIO", "BAJO"]},
            "limite": {"type": "integer", "minimum": 1, "maximum": 40},
        }},
    },
]

_TOOLS = {
    "buscar_contratos": tool_buscar_contratos,
    "ranking_proveedores": tool_ranking_proveedores,
    "resumen_area": tool_resumen_area,
    "buscar_personal": tool_buscar_personal,
    "buscar_cruces": tool_buscar_cruces,
}


def ejecutar_tool(nombre: str, area: str, entrada: dict):
    fn = _TOOLS.get(nombre)
    if not fn:
        return {"error": f"tool desconocida: {nombre}"}
    try:
        return fn(area, **(entrada or {}))
    except TypeError as e:          # parámetro inventado por el modelo
        return {"error": f"parámetros inválidos para {nombre}: {e}"}


SYSTEM_PROMPT_TMPL = """Sos el asistente del Monitor de Transparencia del Poder Ejecutivo Nacional argentino, sección {area_label}.

Datos disponibles hoy en esta área: {n} contratos; {con_razon} con razón social del proveedor y {con_monto} con monto publicados en COMPR.AR ({cob}% de cobertura de monto). El resto solo tiene el título del proceso.

Reglas estrictas:
- Respondé en español, breve y concreto, en TEXTO PLANO: sin tablas, sin markdown, sin asteriscos ni "#". Para listas usá líneas que empiecen con "- ".
- SOLO afirmá datos devueltos por las herramientas. Nunca inventes montos, nombres, CUITs ni fechas.
- Si una búsqueda da 0 resultados, ANTES de responder reintentá vos mismo: con una sola palabra clave distintiva, sin sufijos (SA, SRL), con alcance "ampliada" (JGM) o "todo_pen", y probá las "sugerencias" que devuelva la herramienta. Solo después decí "no encontré coincidencias" y qué probaste.
- Para "mayores proveedores", "top N" o totales usá ranking_proveedores o resumen_area, nunca ordenes a mano una muestra. Informá la cobertura (cuántos contratos tienen monto).
- Si un contrato no tiene razón social ("proveedor": null), decí que COMPR.AR no la publicó y mostrá el objeto y el número de proceso.
- Si total_coincidencias es mayor que lo mostrado, decilo ("se muestran X de Y").
- Toda alerta de cruce es un indicador algorítmico de riesgo, no una acusación.
- Fuera de contratos/personal/cruces de este portal, decí que no es tu función. No des consejos legales ni políticos."""


def system_prompt(area: str) -> str:
    r = tool_resumen_area(area)
    return SYSTEM_PROMPT_TMPL.format(
        area_label=AREA_LABELS[area], n=r["contratos"], con_razon=r["con_razon_social"],
        con_monto=r["con_monto"], cob=r["cobertura_monto_pct"])


# ── Rate limit con IP real ────────────────────────────────────────────────────
_rate: dict[str, tuple[str, int]] = {}


def ip_cliente(request: Request) -> str:
    """IP del cliente detrás del proxy de Railway.

    Sin esto, request.client.host es la IP del proxy y TODOS los visitantes
    comparten el mismo contador diario. Se toma la entrada más a la derecha de
    X-Forwarded-For (la agrega el proxy; las de la izquierda las puede forjar
    el cliente)."""
    xff = request.headers.get("x-forwarded-for", "")
    partes = [p.strip() for p in xff.split(",") if p.strip()]
    if partes:
        return partes[-1]
    return request.headers.get("x-real-ip") or (request.client.host if request.client else "desconocida")


def rate_limit_ok(ip: str) -> bool:
    hoy = datetime.now().strftime("%Y-%m-%d")
    dia, cnt = _rate.get(ip, ("", 0))
    if dia != hoy:
        _rate[ip] = (hoy, 1)
        return True
    if cnt >= CHAT_RATE_LIMIT_DIA:
        return False
    _rate[ip] = (hoy, cnt + 1)
    return True


# ── Llamada a Claude ──────────────────────────────────────────────────────────
async def llamar_claude(client: httpx.AsyncClient, messages: list, system: str) -> dict:
    r = await client.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": ANTHROPIC_MODEL,
            "max_tokens": MAX_TOKENS,
            # El prompt de sistema y las tools son iguales en cada vuelta: se cachean.
            "system": [{"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}],
            "tools": TOOLS_SCHEMA,
            "messages": messages,
        },
        timeout=60,
    )
    r.raise_for_status()
    return r.json()


class ChatIn(BaseModel):
    mensaje: str
    historial: list[dict] = []   # [{"role": "user"|"assistant", "texto": "..."}]


async def chat_v2(area: str, body: ChatIn, request: Request):
    if area not in AREAS_VALIDAS:
        raise HTTPException(status_code=404, detail=f"Área inválida: {area}")
    if not ANTHROPIC_API_KEY:
        raise HTTPException(status_code=503, detail="Agente no configurado (falta ANTHROPIC_API_KEY)")
    mensaje = (body.mensaje or "").strip()
    if not mensaje:
        raise HTTPException(status_code=400, detail="Mensaje vacío")
    if len(mensaje) > 800:
        raise HTTPException(status_code=400, detail="Mensaje demasiado largo (máx. 800 caracteres)")
    if not rate_limit_ok(ip_cliente(request)):
        raise HTTPException(
            status_code=429,
            detail=f"Límite de {CHAT_RATE_LIMIT_DIA} mensajes por día alcanzado. Volvé a intentar mañana.")

    system = system_prompt(area)
    messages = []
    for turno in body.historial[-6:]:
        rol = "assistant" if turno.get("role") == "assistant" else "user"
        texto = str(turno.get("texto", ""))[:800]
        if texto:
            # La API exige alternancia user/assistant: se fusionan turnos seguidos.
            if messages and messages[-1]["role"] == rol:
                messages[-1]["content"] += "\n" + texto
            else:
                messages.append({"role": rol, "content": texto})
    if messages and messages[0]["role"] == "assistant":
        messages.pop(0)
    if messages and messages[-1]["role"] == "user":
        messages[-1]["content"] += "\n" + mensaje
    else:
        messages.append({"role": "user", "content": mensaje})

    usadas: list[str] = []
    try:
        async with httpx.AsyncClient() as client:
            for _ in range(MAX_RONDAS):
                resp = await llamar_claude(client, messages, system)
                bloques = resp.get("content", [])
                if resp.get("stop_reason") != "tool_use":
                    texto = "".join(b.get("text", "") for b in bloques if b.get("type") == "text")
                    return {"respuesta": texto.strip() or "No tengo una respuesta para eso.",
                            "area": area, "version": 2, "herramientas": usadas}
                messages.append({"role": "assistant", "content": bloques})
                resultados = []
                for b in bloques:
                    if b.get("type") != "tool_use":
                        continue
                    usadas.append(b["name"])
                    res = ejecutar_tool(b["name"], area, b.get("input") or {})
                    resultados.append({"type": "tool_result", "tool_use_id": b["id"],
                                       "content": json.dumps(res, ensure_ascii=False, default=str)})
                messages.append({"role": "user", "content": resultados})
        return {"respuesta": "No pude completar la búsqueda, probá con una consulta más simple.",
                "area": area, "version": 2, "herramientas": usadas}
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=502, detail=f"Error del proveedor de IA: {e.response.status_code}")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error inesperado del agente: {type(e).__name__}")


# ── Rutas ─────────────────────────────────────────────────────────────────────
@router.get("/api/v2/chat/diagnostico")
def diagnostico():
    """Cobertura de datos que ve el agente, por área. No expone secretos."""
    return {
        "agente_configurado": bool(ANTHROPIC_API_KEY),
        "modelo": ANTHROPIC_MODEL,
        "rate_limit_diario": CHAT_RATE_LIMIT_DIA,
        "areas": {a: tool_resumen_area(a) for a in sorted(AREAS_VALIDAS)},
        "jgm_ampliada": tool_resumen_area("jgm", "ampliada"),
    }


@router.post("/api/v2/chat/{area}")
async def chat_v2_endpoint(area: str, body: ChatIn, request: Request):
    return await chat_v2(area, body, request)
