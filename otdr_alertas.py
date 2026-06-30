#!/usr/bin/env python3
"""
OTDR Alertas — Detector de queda em tempo real
Consulta SmartOLT (get_outage_pons) a cada 7 min.
Envia email ao detectar nova outage em porta PON.
"""

import os
import sys
import time
import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, timedelta
from pathlib import Path

import json
import requests
from dotenv import load_dotenv

# ── Paths ─────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
LOG_FILE = BASE_DIR / "logs" / "otdr_alertas.log"

# ── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────
load_dotenv(BASE_DIR / ".env")

SMARTOLT_URL  = os.getenv("SMARTOLT_URL", "").rstrip("/")
SMARTOLT_KEY  = os.getenv("SMARTOLT_KEY", "")
HEADERS       = {"X-Token": SMARTOLT_KEY}

SMTP_HOST     = os.getenv("SMTP_HOST",    "smtplw.com.br")
SMTP_PORT     = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER     = os.getenv("SMTP_USER",    "canaatelecom")
SMTP_PASS     = os.getenv("SMTP_PASS",    "Admin01092023")
SMTP_FROM     = os.getenv("SMTP_FROM",    "ti.bsb@canaatelecom.com.br")
ALERT_TO      = os.getenv("OTDR_ALERT_EMAIL", "fernandolima@canaatelecom.com.br")

POLL_INTERVAL = int(os.getenv("OTDR_POLL_INTERVAL", "420"))   # 7 minutos
COOLDOWN_SEC  = int(os.getenv("OTDR_COOLDOWN_SEC",  "7200"))  # 2 horas por OLT

CACHE_FILE    = BASE_DIR / "cache_onus.json"
_olts_cache: list[dict] = []
_olts_ts: float = 0

# ── Estado em memória ─────────────────────────────────────────
_estado_anterior: dict[str, set] = {}     # olt_id → set de chaves de porta em outage
_ultimo_alerta: dict[str, datetime] = {}  # olt_id → timestamp do último alerta
_aquecendo = True  # Primeira rodada: captura baseline sem enviar alertas


# ── SmartOLT API ──────────────────────────────────────────────
def get_olts() -> list[dict]:
    """Retorna lista de OLTs únicos.
    Tenta a API primeiro; fallback no cache local do dashboard OTDR."""
    global _olts_cache, _olts_ts

    # Reusa cache de OLTs por 12h
    if _olts_cache and (time.time() - _olts_ts) < 43200:
        return _olts_cache

    # 1. Tenta API SmartOLT
    try:
        resp = requests.get(f"{SMARTOLT_URL}/api/system/get_olts", headers=HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        olts: list = (
            data if isinstance(data, list)
            else data.get("rows") or data.get("data") or data.get("onus") or []
        )
        if olts:
            log.info(f"OLTs obtidos via API SmartOLT: {len(olts)}")
            _olts_cache, _olts_ts = olts, time.time()
            return _olts_cache
    except Exception as e:
        log.warning(f"get_olts API falhou: {e} — usando cache local")

    # 2. Fallback: extrai OLTs únicos do cache de ONUs do dashboard
    if CACHE_FILE.exists():
        with open(CACHE_FILE) as f:
            cache = json.load(f)
        onus = cache.get("data", [])
        seen: dict[str, dict] = {}
        for o in onus:
            olt_id = str(o.get("olt_id", "")).strip()
            if olt_id and olt_id not in seen:
                seen[olt_id] = {"id": olt_id, "name": o.get("olt_name", olt_id)}
        olts = list(seen.values())
        if olts:
            log.info(f"OLTs extraídos do cache local: {len(olts)}")
            _olts_cache, _olts_ts = olts, time.time()
            return _olts_cache

    return []


def get_outage_pons(olt_id: str) -> list[dict]:
    resp = requests.get(
        f"{SMARTOLT_URL}/api/system/get_outage_pons/{olt_id}",
        headers=HEADERS,
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, list):
        return data
    return data.get("rows") or data.get("outage_pons") or data.get("data") or []


# ── Helpers ───────────────────────────────────────────────────
def chave_porta(porta: dict) -> str:
    board = porta.get("board", porta.get("slot", 0))
    pon   = porta.get("pon_port", porta.get("port", 0))
    return f"{board}:{pon}"


CAUSAS = {
    "FiberCut":           "Fibra cortada",
    "LOS":                "Perda de sinal (LOS)",
    "PowerFail":          "Queda de energia no cliente",
    "OpticalInterference":"Interferência óptica",
}


def causa_texto(raw: str) -> str:
    return CAUSAS.get(raw or "", raw or "Desconhecida")


# ── Email ─────────────────────────────────────────────────────
def enviar_alerta(olt_nome: str, porta: dict, qtd_novas_portas: int) -> None:
    board = porta.get("board", porta.get("slot", "?"))
    pon   = porta.get("pon_port", porta.get("port", "?"))
    onus  = porta.get("onus_count", porta.get("total_onus", "?"))
    los   = porta.get("los_count",  porta.get("los", 0))
    pwrf  = porta.get("power_fail_count", porta.get("power_fail", 0))
    causa = causa_texto(porta.get("outage_cause", porta.get("cause", "")))
    ts    = porta.get("latest_status_change", porta.get("status_change", "—"))
    agora = datetime.now().strftime("%d/%m/%Y %H:%M")

    extra_portas = (
        f"<p style='margin:0 0 16px;color:#b91c1c;font-size:13px;'>"
        f"<strong>+{qtd_novas_portas - 1} outras portas</strong> também entraram em outage neste OLT.</p>"
        if qtd_novas_portas > 1 else ""
    )

    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#f3f4f6;font-family:Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0">
<tr><td align="center" style="padding:24px 12px;">
<table width="560" cellpadding="0" cellspacing="0"
       style="background:#fff;border-radius:8px;border:1px solid #e5e7eb;overflow:hidden;">

  <tr><td style="background:#b91c1c;padding:20px 28px;">
    <p style="margin:0;color:#fff;font-size:17px;font-weight:bold;">⚠️ Queda detectada — {olt_nome}</p>
    <p style="margin:4px 0 0;color:#fca5a5;font-size:12px;">{agora} · OTDR Preventivo Canaã</p>
  </td></tr>

  <tr><td style="padding:24px 28px;">

    {extra_portas}

    <table width="100%" cellpadding="0" cellspacing="0"
           style="border:1px solid #e5e7eb;border-radius:6px;overflow:hidden;font-size:13px;">
      <tr style="background:#fef2f2;">
        <td colspan="2" style="padding:10px 14px;font-weight:bold;color:#991b1b;">
          Porta {board}/{pon}
        </td>
      </tr>
      <tr>
        <td style="padding:9px 14px;color:#6b7280;border-top:1px solid #f3f4f6;width:48%;">Causa</td>
        <td style="padding:9px 14px;font-weight:bold;color:#111827;border-top:1px solid #f3f4f6;">{causa}</td>
      </tr>
      <tr style="background:#f9fafb;">
        <td style="padding:9px 14px;color:#6b7280;">ONUs na porta</td>
        <td style="padding:9px 14px;font-weight:bold;">{onus}</td>
      </tr>
      <tr>
        <td style="padding:9px 14px;color:#6b7280;border-top:1px solid #f3f4f6;">LOS detectados</td>
        <td style="padding:9px 14px;font-weight:bold;color:#dc2626;border-top:1px solid #f3f4f6;">{los}</td>
      </tr>
      <tr style="background:#f9fafb;">
        <td style="padding:9px 14px;color:#6b7280;">Power Fail</td>
        <td style="padding:9px 14px;font-weight:bold;">{pwrf}</td>
      </tr>
      <tr>
        <td style="padding:9px 14px;color:#6b7280;border-top:1px solid #f3f4f6;">Início da ocorrência</td>
        <td style="padding:9px 14px;border-top:1px solid #f3f4f6;">{ts}</td>
      </tr>
    </table>

    <p style="margin:20px 0 0;font-size:11px;color:#9ca3af;">
      Varredura automática · intervalo 7 min · cooldown 2h por OLT.
    </p>

  </td></tr>
</table>
</td></tr>
</table>
</body>
</html>"""

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"⚠️ OTDR · Queda detectada — {olt_nome}"
    msg["From"]    = SMTP_FROM
    msg["To"]      = ALERT_TO
    msg.attach(MIMEText(html, "html"))

    destinatarios = [e.strip() for e in ALERT_TO.split(",")]
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as smtp:
        smtp.starttls()
        smtp.login(SMTP_USER, SMTP_PASS)
        smtp.sendmail(SMTP_FROM, destinatarios, msg.as_string())

    log.info(f"Alerta enviado → {ALERT_TO} | {olt_nome} porta {board}/{pon} | {causa}")


# ── Ciclo principal ───────────────────────────────────────────
def verificar() -> None:
    global _aquecendo

    olts = get_olts()
    if not olts:
        log.warning("get_olts retornou lista vazia.")
        return

    label = " [aquecimento]" if _aquecendo else ""
    log.info(f"Verificando {len(olts)} OLT(s){label}...")

    alertas_enviados = 0

    for olt in olts:
        olt_id   = str(olt.get("id") or olt.get("olt_id") or "")
        olt_nome = olt.get("name") or olt.get("olt_name") or olt_id

        if not olt_id:
            continue

        try:
            portas_outage = get_outage_pons(olt_id)
        except Exception as e:
            log.warning(f"Erro em get_outage_pons({olt_nome}): {e}")
            continue

        chaves_atuais  = {chave_porta(p): p for p in portas_outage}
        chaves_ant     = _estado_anterior.get(olt_id, set())
        novas_portas   = set(chaves_atuais.keys()) - chaves_ant

        if not _aquecendo and novas_portas:
            ultimo = _ultimo_alerta.get(olt_id)
            em_cooldown = ultimo and (datetime.now() - ultimo).total_seconds() < COOLDOWN_SEC

            if em_cooldown:
                retoma = (ultimo + timedelta(seconds=COOLDOWN_SEC)).strftime("%H:%M")
                log.info(f"{olt_nome}: {len(novas_portas)} nova(s) outage(s) — cooldown até {retoma}")
            else:
                porta_principal = chaves_atuais[next(iter(novas_portas))]
                try:
                    enviar_alerta(olt_nome, porta_principal, len(novas_portas))
                    _ultimo_alerta[olt_id] = datetime.now()
                    alertas_enviados += 1
                except Exception as e:
                    log.error(f"Falha ao enviar alerta para {olt_nome}: {e}")

        _estado_anterior[olt_id] = set(chaves_atuais.keys())

    if _aquecendo:
        total = sum(len(v) for v in _estado_anterior.values())
        log.info(f"Baseline capturado: {total} porta(s) em outage em {len(olts)} OLT(s). Monitoramento ativo.")
        _aquecendo = False
    elif alertas_enviados:
        log.info(f"Ciclo concluído: {alertas_enviados} alerta(s) enviado(s).")
    else:
        log.info("Ciclo concluído: sem novas outages.")


# ── Entry point ───────────────────────────────────────────────
def main() -> None:
    if not SMARTOLT_KEY:
        log.error("SMARTOLT_KEY não configurada em .env — abortando.")
        sys.exit(1)

    log.info(f"OTDR Alertas iniciado | URL: {SMARTOLT_URL} | Destino: {ALERT_TO}")
    log.info(f"Intervalo: {POLL_INTERVAL}s | Cooldown: {COOLDOWN_SEC}s")

    while True:
        try:
            verificar()
        except KeyboardInterrupt:
            log.info("Encerrado pelo usuário.")
            break
        except Exception as e:
            log.error(f"Erro no ciclo: {e}")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
