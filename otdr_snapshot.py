#!/usr/bin/env python3
"""
OTDR Preventivo CANAÃ
Snapshot diário: MySQL (IXC) → Postgres (sistema_db)
Caminho: /home/canaa/OTDR/otdr_snapshot.py
"""

import sys
import logging
from datetime import date, datetime
from pathlib import Path

import mysql.connector
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os

# ── Paths ─────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
LOG_FILE = BASE_DIR / "logs" / "otdr.log"
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

# ── Credenciais via .env ──────────────────────────────────────
load_dotenv(ENV_FILE)

MYSQL_CONFIG = {
    "host":               os.getenv("MYSQL_HOST"),
    "database":           os.getenv("MYSQL_DATABASE"),
    "user":               os.getenv("MYSQL_USER"),
    "password":           os.getenv("MYSQL_PASSWORD"),
    "port":               int(os.getenv("MYSQL_PORT", 3306)),
    "connection_timeout": 30,
}

PG_CONFIG = {
    "host":     os.getenv("PG_HOST"),
    "dbname":   os.getenv("PG_DATABASE"),
    "user":     os.getenv("PG_USER"),
    "password": os.getenv("PG_PASSWORD"),
    "port":     int(os.getenv("PG_PORT", 5432)),
}

# ── Query MySQL ───────────────────────────────────────────────
QUERY_MYSQL = """
    SELECT
        t.pop,
        t.pon_descricao,
        t.ponid,
        t.slotno,
        t.sinal_rx,
        t.sinal_tx,
        t.cliente_id,
        t.cliente_razao,
        t.cliente_endereco,
        CASE
            WHEN t.sinal_rx >= -26 THEN '1 - Atencao'
            WHEN t.sinal_rx >= -28 THEN '2 - Critico'
            ELSE '3 - Fora de Operacao'
        END AS nivel_sinal,
        DATEDIFF(NOW(), t.data_sinal) AS dias_degradado,
        CASE
            WHEN t.sinal_rx <  -28 THEN 100
            WHEN t.sinal_rx >= -28 AND t.sinal_rx < -26 THEN 50
            ELSE 10
        END + DATEDIFF(NOW(), t.data_sinal) AS score_urgencia
    FROM (
        SELECT
            pop.pop                 AS pop,
            radio.descricao         AS pon_descricao,
            fibra.ponid             AS ponid,
            fibra.slotno            AS slotno,
            fibra.sinal_rx          AS sinal_rx,
            fibra.sinal_tx          AS sinal_tx,
            fibra.data_sinal        AS data_sinal,
            cli.id                  AS cliente_id,
            cli.razao               AS cliente_razao,
            cli.endereco            AS cliente_endereco,
            ROW_NUMBER() OVER (
                PARTITION BY cli.id
                ORDER BY fibra.data_sinal DESC
            ) AS rn
        FROM radpop_radio_cliente_fibra fibra
        LEFT JOIN radpop_radio  radio ON fibra.id_transmissor = radio.id
        LEFT JOIN radusuarios   usr   ON fibra.id_login       = usr.id
        LEFT JOIN radpop        pop   ON radio.id_pop         = pop.id
        LEFT JOIN cliente       cli   ON usr.id_cliente       = cli.id
        WHERE fibra.sinal_rx IS NOT NULL
          AND fibra.sinal_rx <> ''
          AND fibra.sinal_rx < -24
          AND cli.razao IS NOT NULL
          AND cli.razao <> ''
    ) t
    WHERE t.rn = 1
    ORDER BY score_urgencia DESC
"""

# ── Insert Postgres ───────────────────────────────────────────
INSERT_PG = """
    INSERT INTO otdr.historico_sinal (
        snapshot_data, snapshot_hora,
        cliente_id, cliente_razao, cliente_endereco,
        pop, pon_descricao, ponid, slotno,
        sinal_rx, sinal_tx, nivel_sinal,
        dias_degradado, score_urgencia
    ) VALUES %s
    ON CONFLICT (snapshot_data, cliente_id) DO NOTHING
"""

# ── Main ──────────────────────────────────────────────────────
def main():
    hoje = date.today()
    agora = datetime.now()
    log.info(f"{'='*50}")
    log.info(f"Iniciando snapshot OTDR — {hoje}")

    # 1. Busca no MySQL
    try:
        conn_my = mysql.connector.connect(**MYSQL_CONFIG)
        cur_my  = conn_my.cursor(dictionary=True)
        cur_my.execute(QUERY_MYSQL)
        rows = cur_my.fetchall()
        cur_my.close()
        conn_my.close()
        log.info(f"MySQL: {len(rows)} clientes degradados encontrados")
    except Exception as e:
        log.error(f"Erro MySQL: {e}")
        sys.exit(1)

    if not rows:
        log.warning("Nenhum cliente degradado. Snapshot vazio.")
        sys.exit(0)

    # 2. Monta tuplas para insert em lote
    dados = [
        (
            hoje, agora,
            r["cliente_id"],
            r["cliente_razao"],
            r["cliente_endereco"],
            r["pop"],
            r["pon_descricao"],
            r["ponid"],
            r["slotno"],
            float(r["sinal_rx"])      if r["sinal_rx"]      else None,
            float(r["sinal_tx"])      if r["sinal_tx"]      else None,
            r["nivel_sinal"],
            int(r["dias_degradado"])  if r["dias_degradado"] else 0,
            int(r["score_urgencia"])  if r["score_urgencia"] else 0,
        )
        for r in rows
    ]

    # 3. Insere no Postgres
    try:
        conn_pg = psycopg2.connect(**PG_CONFIG)
        cur_pg  = conn_pg.cursor()
        execute_values(cur_pg, INSERT_PG, dados)
        inseridos = cur_pg.rowcount
        conn_pg.commit()
        cur_pg.close()
        conn_pg.close()
        log.info(f"Postgres: {inseridos} registros inseridos para {hoje}")
    except Exception as e:
        log.error(f"Erro Postgres: {e}")
        sys.exit(1)

    log.info("Snapshot concluído com sucesso.")
    log.info(f"{'='*50}")

if __name__ == "__main__":
    main()
