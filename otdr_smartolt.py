#!/usr/bin/env python3
"""
OTDR Preventivo CANAÃ: snapshot diário de sinal
OLT Cloud → Postgres (sistema_db), tabela otdr.historico_smartolt
Caminho: /home/canaa/OTDR/otdr_smartolt.py

O SmartOLT foi descontinuado em 2026-09 e a fonte passou a ser o OLT Cloud
(oltcloud.py). O nome do arquivo e o da tabela ficaram, porque o cron do root
chama este caminho e o Diagnóstico do Canaã Performance lê essa tabela. As
colunas continuam com o mesmo significado e vocabulário de antes (ver a
tradução em oltcloud.py).

  --dry-run   monta o snapshot e mostra o resumo, sem gravar no Postgres
"""

import sys
import logging
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from datetime import date, datetime
from pathlib import Path
import os

from oltcloud import OltCloud, para_formato_smartolt

# ── Paths ─────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
LOG_FILE = BASE_DIR / "logs" / "otdr_smartolt.log"
ENV_FILE = BASE_DIR / ".env"

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

# ── Credenciais ───────────────────────────────────────────────
load_dotenv(ENV_FILE)


PG_CONFIG = {
    "host":     os.getenv("PG_HOST"),
    "dbname":   os.getenv("PG_DATABASE"),
    "user":     os.getenv("PG_USER"),
    "password": os.getenv("PG_PASSWORD"),
    "port":     int(os.getenv("PG_PORT", 5432)),
}

# ── Insert Postgres ───────────────────────────────────────────
INSERT_PG = """
    INSERT INTO otdr.historico_smartolt (
        snapshot_data, snapshot_hora,
        sn, olt_id, olt_name, board, port, onu,
        zone_name, onu_type,
        sinal_rx, sinal_tx, signal_class,
        status_onu, last_status_change,
        nivel_sinal, dias_degradado
    ) VALUES %s
    ON CONFLICT (snapshot_data, sn) DO NOTHING
"""

def classificar_nivel(sinal_rx):
    if sinal_rx is None:
        return None
    if sinal_rx >= -24:
        return "0 - Normal"
    elif sinal_rx >= -26:
        return "1 - Atencao"
    elif sinal_rx >= -28:
        return "2 - Critico"
    else:
        return "3 - Fora de Operacao"

def main():
    dry_run = "--dry-run" in sys.argv
    hoje = date.today()
    agora = datetime.now()
    log.info("=" * 50)
    log.info(f"Iniciando snapshot de sinal (OLT Cloud) — {hoje}{' [dry-run]' if dry_run else ''}")

    # 1. Busca todas as ONUs no OLT Cloud, já no formato que o snapshot sempre leu
    cliente = OltCloud()
    if not cliente.configurado():
        log.error("OLTCLOUD_URL/OLTCLOUD_USER/OLTCLOUD_PASSWORD não configurados em .env")
        sys.exit(1)
    try:
        log.info(f"Consultando OLT Cloud: {cliente.url}")
        # ONU fora do ar entra com a última leitura válida (ver para_formato_smartolt)
        onus = [para_formato_smartolt(e, manter_ultimo_sinal=True) for e in cliente.equipamentos()]
        log.info(f"OLT Cloud: {len(onus)} ONUs recebidas")
    except Exception as e:
        log.error(f"Erro ao consultar OLT Cloud: {e}")
        sys.exit(1)

    if not onus:
        log.warning("Nenhuma ONU retornada. Abortando.")
        sys.exit(0)

    # 2. Filtra ONUs com sinal degradado e monta tuplas
    degradadas = 0
    dados = []
    for o in onus:
        rx = o.get("signal_1310")
        tx = o.get("signal_1490")

        try:
            rx = float(rx) if rx not in (None, "", "null") else None
        except (ValueError, TypeError):
            rx = None

        try:
            tx = float(tx) if tx not in (None, "", "null") else None
        except (ValueError, TypeError):
            tx = None

        nivel = classificar_nivel(rx)

        # Salva apenas ONUs degradadas
        if nivel in ("1 - Atencao", "2 - Critico", "3 - Fora de Operacao"):
            degradadas += 1

            last_change = o.get("last_status_change")
            try:
                last_change = datetime.strptime(last_change, "%Y-%m-%d %H:%M:%S") if last_change else None
            except (ValueError, TypeError):
                last_change = None

            dias = (hoje - last_change.date()).days if last_change else 0

            dados.append((
                hoje, agora,
                o.get("sn"),
                o.get("olt_id"),
                o.get("olt_name"),
                o.get("board"),
                o.get("port"),
                o.get("onu"),
                o.get("zone_name"),
                o.get("onu_type_name"),
                rx, tx,
                o.get("signal"),
                o.get("status"),
                last_change,
                nivel,
                dias,
            ))

    log.info(f"ONUs degradadas (sinal < -24 dBm): {degradadas}")

    if dry_run:
        from collections import Counter
        log.info(f"[dry-run] nível: {dict(Counter(d[15] for d in dados))} | status: {dict(Counter(d[13] for d in dados))}")
        log.info("[dry-run] nada gravado no Postgres.")
        return

    if not dados:
        log.warning("Nenhuma ONU degradada encontrada.")
        sys.exit(0)

    # 3. Insere no Postgres
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        cur = conn.cursor()
        execute_values(cur, INSERT_PG, dados)
        inseridos = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()
        log.info(f"Postgres: {inseridos} registros inseridos para {hoje}")
    except Exception as e:
        log.error(f"Erro ao inserir no Postgres: {e}")
        sys.exit(1)

    log.info("Snapshot de sinal (OLT Cloud) concluído com sucesso.")
    log.info("=" * 50)

if __name__ == "__main__":
    main()
