#!/usr/bin/env python3
"""
OTDR Dashboard Interno — CANAÃ Telecom v2
Flask app — SmartOLT + MySQL IXC + PostgreSQL histórico
Porta: 5008
"""

from flask import Flask, jsonify, render_template, request
from dotenv import load_dotenv
from datetime import datetime, date
import requests
import mysql.connector
import psycopg2
import psycopg2.extras
import os
import time
import threading
from pathlib import Path
import json

# ── Config ────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

SMARTOLT_URL = os.getenv("SMARTOLT_URL")
SMARTOLT_KEY = os.getenv("SMARTOLT_KEY")
HEADERS      = {"X-Token": SMARTOLT_KEY}

MYSQL_CONFIG = {
    "host":     os.getenv("MYSQL_HOST"),
    "database": os.getenv("MYSQL_DATABASE"),
    "user":     os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
    "port":     int(os.getenv("MYSQL_PORT", 3306)),
    "connection_timeout": 30,
}

PG_CONFIG = {
    "host":     os.getenv("PG_HOST"),
    "dbname":   os.getenv("PG_DATABASE"),
    "user":     os.getenv("PG_USER"),
    "password": os.getenv("PG_PASSWORD"),
    "port":     int(os.getenv("PG_PORT", 5432)),
}

app = Flask(__name__)

# ── Constantes de nível (padrão único em todo o sistema) ──────
NIVEL_NORMAL  = "Normal"
NIVEL_ATENCAO = "Atencao"
NIVEL_CRITICO = "Critico"
NIVEL_FORA    = "Fora de Operacao"
NIVEL_SEM     = "Sem leitura"

def classificar(rx):
    if rx is None: return NIVEL_SEM
    rx = float(rx)
    if rx >= -24:   return NIVEL_NORMAL
    elif rx >= -26: return NIVEL_ATENCAO
    elif rx >= -28: return NIVEL_CRITICO
    else:           return NIVEL_FORA

# ── Cache SmartOLT — thread-safe + persistência em disco ──────
CACHE_FILE       = BASE_DIR / "cache_onus.json"
CACHE_TTL        = 86400  # 24h — cache renovado pelo cron ou refresh manual
CACHE_TTL_MIN    = 300    # cooldown mínimo para ?live=1 (5 min)
_cache_lock      = threading.Lock()

def _load_cache_disco():
    """Carrega cache do disco ao iniciar — garante dados mesmo após restart."""
    try:
        if CACHE_FILE.exists():
            with open(CACHE_FILE) as f:
                salvo = json.load(f)
            onus = salvo.get("data")
            ts   = salvo.get("timestamp", 0)
            if onus:
                import logging
                logging.getLogger(__name__).info(
                    f"Cache carregado do disco: {len(onus)} ONUs "
                    f"(salvo {int((time.time()-ts)//60)} min atrás)"
                )
                return {"data": onus, "timestamp": ts}
    except Exception as e:
        pass
    return {"data": None, "timestamp": 0}

_cache = _load_cache_disco()

# ── Cache mapeamento MAC→cliente (IXC) — thread-safe ─────────
_mac_cache      = {"data": None, "timestamp": 0}
_mac_cache_lock = threading.Lock()
MAC_CACHE_TTL   = 3600  # 1 hora

def get_mysql():
    return mysql.connector.connect(**MYSQL_CONFIG)

def get_pg():
    return psycopg2.connect(**PG_CONFIG)

def get_mac_map():
    agora = time.time()
    with _mac_cache_lock:
        if _mac_cache["data"] and (agora - _mac_cache["timestamp"]) < MAC_CACHE_TTL:
            return _mac_cache["data"]
    conn = None
    try:
        conn = get_mysql()
        cur  = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT f.mac, c.id AS cliente_id, c.razao AS cliente_nome
            FROM radpop_radio_cliente_fibra f
            JOIN radusuarios u ON u.id = f.id_login
            JOIN cliente c ON c.id = u.id_cliente
            WHERE f.mac IS NOT NULL AND f.mac <> ''
        """)
        mapa = {r["mac"].upper(): {"id": r["cliente_id"], "nome": r["cliente_nome"]} for r in cur.fetchall()}
        cur.close()
        with _mac_cache_lock:
            _mac_cache["data"] = mapa
            _mac_cache["timestamp"] = agora
        return mapa
    except Exception as e:
        app.logger.error(f"get_mac_map falhou: {e}")
        with _mac_cache_lock:
            return _mac_cache["data"] or {}
    finally:
        if conn:
            try: conn.close()
            except: pass

def get_onus(force=False):
    agora = time.time()
    with _cache_lock:
        tem_cache = bool(_cache["data"])
        valido    = tem_cache and (agora - _cache["timestamp"]) < CACHE_TTL

    # Sem force → sempre retorna cache se existir (page load normal)
    if tem_cache and not force:
        with _cache_lock:
            return _cache["data"], False, False

    # force=True ou sem cache → chama a API
    try:
        url = f"{SMARTOLT_URL}/api/onu/get_all_onus_details"
        resp = requests.get(url, headers=HEADERS, timeout=120)
        resp.raise_for_status()
        onus = resp.json().get("onus", [])
        with _cache_lock:
            _cache["data"]      = onus
            _cache["timestamp"] = agora
        try:
            with open(CACHE_FILE, "w") as f:
                json.dump({"data": onus, "timestamp": agora}, f)
        except Exception as e:
            app.logger.warning(f"Falha ao salvar cache em disco: {e}")
        return onus, True, False
    except Exception as e:
        # Rate limit ou erro — retorna cache se disponível
        with _cache_lock:
            dados = _cache.get("data")
        if dados:
            app.logger.warning(f"SmartOLT indisponível ({e}) — usando cache ({len(dados)} ONUs)")
            return dados, False, True   # rate_limited=True
        raise

# ── IDs assuntos técnicos ─────────────────────────────────────
ASSUNTOS_IDS = (
    215,256,316,317,318,423,463,518,519,520,
    569,580,665,685,692,200,703,705,612
)
ASSUNTOS_STR = ",".join(str(i) for i in ASSUNTOS_IDS)

# ── Páginas ───────────────────────────────────────────────────
@app.route("/")
def index():    return render_template("index.html")

@app.route("/os")
def os_page():  return render_template("os.html")

@app.route("/kpis")
def kpis_page(): return render_template("kpis.html")

@app.route("/historico")
def hist_page(): return render_template("historico.html")

# ── API SmartOLT ──────────────────────────────────────────────
@app.route("/api/onus")
def api_onus():
    try:
        force = request.args.get('live') == '1'
        onus, atualizado, _rl = get_onus(force=force)
        mac_map = get_mac_map()
        resultado = []
        for o in onus:
            rx = o.get("signal_1310")
            tx = o.get("signal_1490")
            try: rx = float(rx) if rx not in (None,"","null") else None
            except: rx = None
            try: tx = float(tx) if tx not in (None,"","null") else None
            except: tx = None
            nivel = classificar(rx)
            sn_upper = (o.get("sn") or "").upper()
            cli = mac_map.get(sn_upper, {})
            resultado.append({
                "sn":o.get("sn"),"olt":o.get("olt_name"),
                "board":o.get("board"),"port":o.get("port"),
                "onu":o.get("onu"),"zone":o.get("zone_name"),
                "tipo":o.get("onu_type_name"),"status":o.get("status"),
                "sinal_rx":rx,"sinal_tx":tx,
                "signal_class":o.get("signal"),"nivel":nivel,
                "ultima_mudanca":o.get("last_status_change"),
                "cliente_id":  cli.get("id",""),
                "cliente_nome":cli.get("nome",""),
                "falha_sync":  str(o.get("is_failed_resync_config","0")) == "1",
                "desabilitada":str(o.get("administrative_status","Enabled")).lower() != "enabled",
            })

        # Resumo em loop único — O(n) em vez de O(n*8)
        res = {"total":len(resultado),"online":0,"offline":0,"offline_puro":0,
               "los":0,"pwrfail":0,"normal":0,"atencao":0,
               "critico":0,"fora":0,"sem_leit":0,
               "falha_sync":0,"desabilitadas":0}
        for r in resultado:
            st = r["status"]; nv = r["nivel"]
            if st == "Online":       res["online"]        += 1
            else:                    res["offline"]       += 1
            if st == "Offline":      res["offline_puro"]  += 1
            elif st == "LOS":        res["los"]           += 1
            elif st == "Power fail": res["pwrfail"]       += 1
            # Sinal: conta apenas ONUs online (Opção A)
            if st == "Online":
                if nv == NIVEL_NORMAL:   res["normal"]    += 1
                elif nv == NIVEL_ATENCAO:res["atencao"]   += 1
                elif nv == NIVEL_CRITICO:res["critico"]   += 1
                elif nv == NIVEL_FORA:   res["fora"]      += 1
                else:                    res["sem_leit"]  += 1
            if r["falha_sync"]:      res["falha_sync"]    += 1
            if r["desabilitada"]:    res["desabilitadas"] += 1

        cache_age = int(time.time() - _cache["timestamp"])
        return jsonify({
            "status":"ok","atualizado":atualizado,"cache_age":cache_age,
            "timestamp":datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
            "resumo":res,
            "onus":resultado,
        })
    except Exception as e:
        return jsonify({"status":"erro","mensagem":str(e)}),500


@app.route("/api/cliente_sinal/<int:cliente_id>")
def api_cliente_sinal(cliente_id):
    """Retorna ONUs de um cliente específico. ?live=1 força refresh do cache."""
    try:
        # live=1 é tratado via force=True em get_onus()
        onus, _, _rl = get_onus()
        mac_map  = get_mac_map()
        # Inverte mac_map para id → lista de SNs
        resultado = []
        for o in onus:
            sn_upper = (o.get("sn") or "").upper()
            cli = mac_map.get(sn_upper, {})
            if cli.get("id") == cliente_id:
                rx = o.get("signal_1310")
                tx = o.get("signal_1490")
                try: rx = float(rx) if rx not in (None,"","null") else None
                except: rx = None
                try: tx = float(tx) if tx not in (None,"","null") else None
                except: tx = None
                resultado.append({
                    "sn":       o.get("sn"),
                    "olt":      o.get("olt_name"),
                    "board":    o.get("board"),
                    "port":     o.get("port"),
                    "onu":      o.get("onu"),
                    "zone":     o.get("zone_name"),
                    "tipo":     o.get("onu_type_name"),
                    "status":   o.get("status"),
                    "sinal_rx": rx,
                    "sinal_tx": tx,
                    "nivel":    classificar(rx),
                    "ultima_mudanca": o.get("last_status_change"),
                    "cliente_nome": cli.get("nome",""),
                })
        return jsonify({"status":"ok","cliente_id":cliente_id,"onus":resultado})
    except Exception as e:
        return jsonify({"status":"erro","mensagem":str(e)}),500

@app.route("/api/refresh")
def api_refresh():
    _cache["timestamp"] = 0
    return jsonify({"status":"ok"})

# ── API OS ────────────────────────────────────────────────────
@app.route("/api/os")
def api_os():
    try:
        dt_ini  = request.args.get("dt_ini","2025-01-01")
        dt_fim  = request.args.get("dt_fim", date.today().isoformat())
        pop     = request.args.get("pop","")
        assunto = request.args.get("assunto","")

        where = ""
        params = [dt_ini, dt_fim]
        if assunto:
            where += " AND os.id_assunto = %s"
            params.append(int(assunto))
        else:
            where += f" AND os.id_assunto IN ({ASSUNTOS_STR})"
        if pop:
            where += " AND pop.pop = %s"
            params.append(pop)

        query = f"""
            SELECT
                os.id                        AS id_atd,
                os.id_cliente,
                ass.assunto,
                cli.razao                    AS nome,
                cli.bairro,
                cli.endereco,
                fibra.onu_tipo               AS onu_tipo,
                pop.pop,
                os.data_abertura,
                os.data_fechamento,
                os.status,
                fibra.sinal_rx,
                fibra.sinal_tx
            FROM su_oss_chamado os
            LEFT JOIN su_oss_assunto ass ON os.id_assunto = ass.id
            LEFT JOIN cliente cli        ON os.id_cliente = cli.id
            LEFT JOIN (
                SELECT u.id_cliente, MAX(f.id) AS max_fibra_id
                FROM radusuarios u
                JOIN radpop_radio_cliente_fibra f ON f.id_login = u.id
                GROUP BY u.id_cliente
            ) mf ON mf.id_cliente = cli.id
            LEFT JOIN radpop_radio_cliente_fibra fibra ON fibra.id = mf.max_fibra_id
            LEFT JOIN radpop_radio radio ON radio.id = fibra.id_transmissor
            LEFT JOIN radpop pop ON pop.id = radio.id_pop
            WHERE os.data_abertura BETWEEN %s AND %s
              AND cli.razao IS NOT NULL
              AND cli.razao <> 'TESTE RMA'
              {where}
            GROUP BY os.id
            ORDER BY os.data_abertura DESC
            LIMIT 2000
        """
        conn = get_mysql()
        try:
            cur = conn.cursor(dictionary=True)
            cur.execute(query, params)
            rows = cur.fetchall()
            cur.close()
        finally:
            conn.close()
        for r in rows:
            for k,v in r.items():
                if isinstance(v, datetime): r[k] = v.strftime("%d/%m/%Y %H:%M")
                elif v is None: r[k] = ""
            for k in ("sinal_rx", "sinal_tx"):
                if r.get(k) == 0.0 or r.get(k) == "0.00": r[k] = ""
        return jsonify({"status":"ok","total":len(rows),"dados":rows})
    except Exception as e:
        return jsonify({"status":"erro","mensagem":str(e)}),500

@app.route("/api/os/assuntos")
def api_assuntos():
    try:
        conn = get_mysql()
        try:
            cur = conn.cursor(dictionary=True)
            cur.execute(f"SELECT id, assunto FROM su_oss_assunto WHERE id IN ({ASSUNTOS_STR}) ORDER BY assunto")
            rows = cur.fetchall()
            cur.close()
        finally:
            conn.close()
        return jsonify({"status":"ok","dados":rows})
    except Exception as e:
        return jsonify({"status":"erro","mensagem":str(e)}),500

@app.route("/api/os/pops")
def api_pops():
    try:
        conn = get_mysql()
        try:
            cur = conn.cursor(dictionary=True)
            cur.execute("SELECT DISTINCT pop FROM radpop WHERE pop IS NOT NULL ORDER BY pop")
            rows = cur.fetchall()
            cur.close()
        finally:
            conn.close()
        return jsonify({"status":"ok","dados":[r["pop"] for r in rows]})
    except Exception as e:
        return jsonify({"status":"erro","mensagem":str(e)}),500

# ── API KPIs ──────────────────────────────────────────────────
@app.route("/api/kpis")
def api_kpis():
    try:
        onus, _, _rl = get_onus()
        t = len(onus)
        online  = sum(1 for o in onus if o.get("status")=="Online")
        offline = t - online
        sinal = {"normal":0,"atencao":0,"critico":0,"fora":0,"sem":0}
        por_olt = {}
        falha_sync = 0
        desabilitadas = 0
        for o in onus:
            rx = o.get("signal_1310")
            try: rx = float(rx) if rx not in (None,"","null") else None
            except: rx = None
            nivel = classificar(rx)
            olt = o.get("olt_name","Desconhecida")
            is_online = o.get("status") == "Online"
            if olt not in por_olt:
                por_olt[olt] = {"olt":olt,"total":0,"online":0,"offline":0,"critico":0,"fora":0}
            por_olt[olt]["total"] += 1
            if is_online: por_olt[olt]["online"] += 1
            else:         por_olt[olt]["offline"] += 1
            # Sinal: conta apenas ONUs online (Opção A)
            if is_online:
                key = {NIVEL_NORMAL:"normal",NIVEL_ATENCAO:"atencao",NIVEL_CRITICO:"critico",
                       NIVEL_FORA:"fora",NIVEL_SEM:"sem"}[nivel]
                sinal[key] += 1
                if nivel==NIVEL_CRITICO: por_olt[olt]["critico"] += 1
                if nivel==NIVEL_FORA:    por_olt[olt]["fora"]    += 1
            if str(o.get("is_failed_resync_config","0")) == "1": falha_sync    += 1
            if str(o.get("administrative_status","Enabled")).lower() != "enabled": desabilitadas += 1

        conn = get_mysql()
        try:
            cur = conn.cursor(dictionary=True)
            cur.execute(f"SELECT COUNT(*) AS qtd FROM su_oss_chamado WHERE DATE(data_abertura)=CURDATE() AND id_assunto IN ({ASSUNTOS_STR})")
            abertos_hoje = cur.fetchone()["qtd"]
            cur.execute(f"SELECT COUNT(*) AS qtd FROM su_oss_chamado WHERE status IN ('A','AS') AND id_assunto IN ({ASSUNTOS_STR})")
            em_aberto = cur.fetchone()["qtd"]
            cur.execute(f"""
                SELECT ass.assunto, COUNT(*) AS qtd
                FROM su_oss_chamado os
                LEFT JOIN su_oss_assunto ass ON os.id_assunto=ass.id
                WHERE os.data_abertura >= DATE_SUB(NOW(),INTERVAL 30 DAY)
                  AND os.id_assunto IN ({ASSUNTOS_STR})
                GROUP BY ass.assunto ORDER BY qtd DESC LIMIT 10""")
            por_assunto = cur.fetchall()
            cur.execute(f"""
                SELECT pop.pop, COUNT(*) AS qtd
                FROM su_oss_chamado os
                LEFT JOIN cliente cli ON os.id_cliente=cli.id
                LEFT JOIN radusuarios usr ON usr.id_cliente=cli.id
                LEFT JOIN radpop_radio_cliente_fibra fibra ON fibra.id_login=usr.id
                LEFT JOIN radpop_radio radio ON fibra.id_transmissor=radio.id
                LEFT JOIN radpop pop ON radio.id_pop=pop.id
                WHERE os.data_abertura >= DATE_SUB(NOW(),INTERVAL 30 DAY)
                  AND os.id_assunto IN ({ASSUNTOS_STR})
                  AND pop.pop IS NOT NULL AND cli.razao <> 'TESTE RMA'
                GROUP BY pop.pop ORDER BY qtd DESC""")
            por_pop = cur.fetchall()
            cur.close()
        finally:
            conn.close()

        return jsonify({
            "status":"ok",
            "timestamp":datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
            "rede":{"total_onus":t,"online":online,"offline":offline,
                    "pct_online":round(online/t*100,1) if t else 0,
                    "falha_sync":falha_sync,"desabilitadas":desabilitadas,**sinal},
            "os":{"abertos_hoje":abertos_hoje,"em_aberto":em_aberto,
                  "por_assunto":por_assunto,"por_pop":por_pop},
            "por_olt":sorted(por_olt.values(),key=lambda x:-x["total"]),
        })
    except Exception as e:
        return jsonify({"status":"erro","mensagem":str(e)}),500

# ── API Histórico ─────────────────────────────────────────────

def _hist_datas():
    """Retorna (dt_ini, dt_fim) como strings YYYY-MM-DD a partir dos query params."""
    from datetime import date, timedelta
    hoje = date.today()
    ini  = request.args.get("dtIni", "")
    fim  = request.args.get("dtFim", "")
    # Validação simples
    import re
    pat = r'^\d{4}-\d{2}-\d{2}$'
    if not re.match(pat, ini):
        ini = (hoje - timedelta(days=30)).isoformat()
    if not re.match(pat, fim):
        fim = hoje.isoformat()
    return ini, fim

@app.route("/api/historico/evolucao")
def api_hist_evolucao():
    """Evolução diária por OLT — total degradados por nível"""
    try:
        conn = get_pg()
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            dt_ini, dt_fim = _hist_datas()
            cur.execute("""
                SELECT
                    snapshot_data,
                    olt_name,
                    nivel_sinal,
                    COUNT(*) AS qtd,
                    ROUND(AVG(sinal_rx)::numeric,2) AS media_rx,
                    MIN(sinal_rx) AS pior_rx
                FROM otdr.historico_smartolt
                WHERE snapshot_data BETWEEN %s AND %s
                GROUP BY snapshot_data, olt_name, nivel_sinal
                ORDER BY snapshot_data DESC, olt_name
            """, (dt_ini, dt_fim))
            rows = [dict(r) for r in cur.fetchall()]
            cur.close()
        finally:
            conn.close()
        for r in rows:
            if r.get("snapshot_data"):
                r["snapshot_data"] = r["snapshot_data"].strftime("%d/%m/%Y")
        return jsonify({"status":"ok","dados":rows})
    except Exception as e:
        return jsonify({"status":"erro","mensagem":str(e)}),500

@app.route("/api/historico/piora")
def api_hist_piora():
    """ONUs que pioraram o nível de sinal entre snapshots"""
    try:
        conn = get_pg()
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            dt_ini, dt_fim = _hist_datas()
            cur.execute("""
                WITH ranked AS (
                    SELECT
                        sn, olt_name, zone_name, onu_type,
                        snapshot_data, sinal_rx, nivel_sinal,
                        LAG(sinal_rx) OVER (PARTITION BY sn ORDER BY snapshot_data) AS rx_anterior,
                        LAG(nivel_sinal) OVER (PARTITION BY sn ORDER BY snapshot_data) AS nivel_anterior,
                        LAG(snapshot_data) OVER (PARTITION BY sn ORDER BY snapshot_data) AS data_anterior
                    FROM otdr.historico_smartolt
                    WHERE snapshot_data BETWEEN %s AND %s
                )
                SELECT *
                FROM ranked
                WHERE rx_anterior IS NOT NULL
                  AND sinal_rx < rx_anterior
                  AND sinal_rx < -24
                ORDER BY (sinal_rx - rx_anterior) ASC
                LIMIT 100
            """, (dt_ini, dt_fim))
            rows = [dict(r) for r in cur.fetchall()]
            cur.close()
        finally:
            conn.close()
        for r in rows:
            for k in ("snapshot_data","data_anterior"):
                if r.get(k): r[k] = r[k].strftime("%d/%m/%Y")
            for k in ("sinal_rx","rx_anterior"):
                if r.get(k) is not None: r[k] = float(r[k])
        return jsonify({"status":"ok","dados":rows})
    except Exception as e:
        return jsonify({"status":"erro","mensagem":str(e)}),500

@app.route("/api/historico/offline")
def api_hist_offline():
    """ONUs que aparecem offline com frequência no histórico"""
    try:
        conn = get_pg()
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            dt_ini, dt_fim = _hist_datas()
            cur.execute("""
                SELECT
                    sn, olt_name, zone_name, onu_type,
                    COUNT(*) AS dias_degradado,
                    MIN(sinal_rx) AS pior_rx,
                    ROUND(AVG(sinal_rx)::numeric,2) AS media_rx,
                    MAX(snapshot_data) AS ultima_data,
                    MIN(snapshot_data) AS primeira_data,
                    MODE() WITHIN GROUP (ORDER BY nivel_sinal) AS nivel_predominante
                FROM otdr.historico_smartolt
                WHERE snapshot_data BETWEEN %s AND %s
                GROUP BY sn, olt_name, zone_name, onu_type
                HAVING COUNT(*) > 1
                ORDER BY dias_degradado DESC, pior_rx ASC
                LIMIT 200
            """, (dt_ini, dt_fim))
            rows = [dict(r) for r in cur.fetchall()]
            cur.close()
        finally:
            conn.close()
        for r in rows:
            for k in ("ultima_data","primeira_data"):
                if r.get(k): r[k] = r[k].strftime("%d/%m/%Y")
            for k in ("pior_rx","media_rx"):
                if r.get(k) is not None: r[k] = float(r[k])
        return jsonify({"status":"ok","dados":rows})
    except Exception as e:
        return jsonify({"status":"erro","mensagem":str(e)}),500

@app.route("/api/historico/resumo")
def api_hist_resumo():
    """Resumo do histórico por dia"""
    try:
        conn = get_pg()
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            dt_ini, dt_fim = _hist_datas()
            cur.execute("""
                SELECT
                    snapshot_data,
                    COUNT(*) AS total_degradados,
                    SUM(CASE WHEN nivel_sinal='1 - Atencao'         THEN 1 ELSE 0 END) AS atencao,
                    SUM(CASE WHEN nivel_sinal='2 - Critico'          THEN 1 ELSE 0 END) AS critico,
                    SUM(CASE WHEN nivel_sinal='3 - Fora de Operacao' THEN 1 ELSE 0 END) AS fora,
                    ROUND(AVG(sinal_rx)::numeric,2) AS media_rx,
                    MIN(sinal_rx) AS pior_rx
                FROM otdr.historico_smartolt
                WHERE snapshot_data BETWEEN %s AND %s
                GROUP BY snapshot_data
                ORDER BY snapshot_data DESC
            """, (dt_ini, dt_fim))
            rows = [dict(r) for r in cur.fetchall()]
            cur.close()
        finally:
            conn.close()
        for r in rows:
            if r.get("snapshot_data"): r["snapshot_data"] = r["snapshot_data"].strftime("%d/%m/%Y")
            for k in ("media_rx","pior_rx"):
                if r.get(k) is not None: r[k] = float(r[k])
        return jsonify({"status":"ok","dados":rows})
    except Exception as e:
        return jsonify({"status":"erro","mensagem":str(e)}),500


# ── Mapa de Calor ─────────────────────────────────────────────
@app.route("/mapa")
def mapa_page(): return render_template("mapa.html")

@app.route("/api/mapa")
def api_mapa():
    """
    Retorna pontos georreferenciados (clientes fibra com lat/lon no IXC)
    cruzados com nível de sinal atual (cache SmartOLT).
    Também retorna POPs/OLTs para marcadores de referência.
    """
    try:
        # 1. Pegar ONUs do cache SmartOLT
        force = request.args.get('refresh') == '1'
        onus, _, _rl = get_onus(force=force)
        mac_map  = get_mac_map()

        # Montar dict: cliente_id → melhor nivel (para clientes com múltiplas ONUs)
        ORDEM = {NIVEL_FORA: 4, NIVEL_CRITICO: 3, NIVEL_ATENCAO: 2, NIVEL_NORMAL: 1, NIVEL_SEM: 0}
        cliente_nivel = {}   # cliente_id → {nivel, rx, status, sn}
        for onu in onus:
            sn  = (onu.get("sn") or "").upper()
            info = mac_map.get(sn)
            if not info:
                continue
            cid   = info["id"]
            nome  = info["nome"]
            rx    = onu.get("signal_1310")
            try:   rx = float(rx)
            except: rx = None
            nivel  = classificar(rx)
            status = onu.get("onu_status", "")
            atual  = cliente_nivel.get(cid)
            if not atual or ORDEM.get(nivel, 0) > ORDEM.get(atual["nivel"], 0):
                cliente_nivel[cid] = {
                    "cliente_id":   cid,
                    "cliente_nome": nome,
                    "nivel":        nivel,
                    "rx":           round(rx, 2) if rx is not None else None,
                    "status":       status,
                    "sn":           sn,
                }

        if not cliente_nivel:
            return jsonify({"status": "ok", "pontos": [], "pops": []})

        # 2. Buscar lat/lon do IXC apenas para clientes que estão no cache
        ids_list = list(cliente_nivel.keys())
        conn = None
        pontos = []
        pops   = []
        try:
            conn = get_mysql()
            cur  = conn.cursor(dictionary=True)

            # Clientes com coordenadas
            fmt = ",".join(["%s"] * len(ids_list))
            cur.execute(f"""
                SELECT id, latitude, longitude
                FROM cliente
                WHERE id IN ({fmt})
                  AND latitude  IS NOT NULL AND latitude  != '' AND latitude  != '0'
                  AND longitude IS NOT NULL AND longitude != '' AND longitude != '0'
            """, ids_list)
            for row in cur.fetchall():
                cid = row["id"]
                try:
                    lat = float(row["latitude"])
                    lon = float(row["longitude"])
                except:
                    continue
                if lat == 0 or lon == 0:
                    continue
                info = cliente_nivel.get(cid)
                if not info:
                    continue
                pontos.append({
                    "lat":          lat,
                    "lon":          lon,
                    "cliente_id":   info["cliente_id"],
                    "cliente_nome": info["cliente_nome"],
                    "nivel":        info["nivel"],
                    "rx":           info["rx"],
                    "status":       info["status"],
                    "sn":           info["sn"],
                })

            # POPs/OLTs como marcadores de referência
            cur.execute("""
                SELECT pop, bairro, latitude, longitude
                FROM radpop
                WHERE latitude  IS NOT NULL AND latitude  != '' AND latitude  != '0'
                  AND longitude IS NOT NULL AND longitude != '' AND longitude != '0'
            """)
            for row in cur.fetchall():
                try:
                    lat = float(row["latitude"])
                    lon = float(row["longitude"])
                except:
                    continue
                pops.append({
                    "lat":    lat,
                    "lon":    lon,
                    "nome":   row["pop"]   or "",
                    "bairro": row["bairro"] or "",
                })
            cur.close()
        finally:
            if conn:
                try: conn.close()
                except: pass

        return jsonify({
            "status":       "ok",
            "total":        len(pontos),
            "pontos":       pontos,
            "pops":         pops,
            "rate_limited": _rl,
            "cache_ts":     _cache.get("timestamp", 0),
        })

    except Exception as e:
        msg = str(e)
        app.logger.error(f"api_mapa erro: {e}")
        if "403" in msg or "rate" in msg.lower() or "Forbidden" in msg:
            return jsonify({"status": "rate_limit", "mensagem": "SmartOLT rate limit excedido. Aguarde e tente novamente."}), 503
        return jsonify({"status": "erro", "mensagem": msg}), 500

# ── Alertas: status de outage em tempo real ───────────────────
_alertas_cache: dict = {"data": None, "ts": 0}
_ALERTAS_TTL = 120  # 2 min

CAUSAS_MAP = {
    "FiberCut":            "Fibra cortada",
    "LOS":                 "LOS",
    "PowerFail":           "Power Fail",
    "OpticalInterference": "Interferência óptica",
}

@app.route("/api/alertas/status")
def api_alertas_status():
    global _alertas_cache
    agora = time.time()
    if _alertas_cache["data"] is not None and (agora - _alertas_cache["ts"]) < _ALERTAS_TTL:
        d = dict(_alertas_cache["data"])
        d["cached"] = True
        return jsonify(d)

    try:
        with _cache_lock:
            onus = list(_cache.get("data") or [])
        if not onus and CACHE_FILE.exists():
            with open(CACHE_FILE) as f:
                onus = json.load(f).get("data", [])

        olts: dict = {}
        for o in onus:
            oid = str(o.get("olt_id", "")).strip()
            if oid and oid not in olts:
                olts[oid] = o.get("olt_name", oid)

        outages = []
        for olt_id, olt_nome in olts.items():
            try:
                r = requests.get(
                    f"{SMARTOLT_URL}/api/system/get_outage_pons/{olt_id}",
                    headers=HEADERS, timeout=10,
                )
                r.raise_for_status()
                d = r.json()
                portas = d.get("rows") or d.get("outage_pons") or d.get("data") or []
                for p in portas:
                    causa_raw = p.get("outage_cause", p.get("cause", ""))
                    outages.append({
                        "olt_id":   olt_id,
                        "olt_nome": olt_nome,
                        "board":    p.get("board"),
                        "port":     p.get("pon_port", p.get("port")),
                        "onus":     p.get("onus_count", p.get("total_onus")),
                        "los":      p.get("los_count",  p.get("los", 0)),
                        "pwrfail":  p.get("power_fail_count", p.get("power_fail", 0)),
                        "causa":    CAUSAS_MAP.get(causa_raw, causa_raw or "Desconhecida"),
                        "desde":    p.get("latest_status_change", ""),
                    })
            except Exception:
                pass

        resultado = {
            "status":  "ok",
            "outages": outages,
            "total":   len(outages),
            "ts":      datetime.now().strftime("%H:%M"),
            "cached":  False,
        }
        _alertas_cache = {"data": resultado, "ts": agora}
        return jsonify(resultado)

    except Exception as e:
        return jsonify({"status": "error", "mensagem": str(e), "outages": [], "total": 0}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5008, debug=False)
