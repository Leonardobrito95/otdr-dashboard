#!/usr/bin/env python3
"""
OTDR Dashboard Interno — CANAÃ Telecom v2
Flask app — SmartOLT + MySQL IXC + PostgreSQL histórico
Porta: 5008
"""

from flask import Flask, jsonify, render_template, request, session, redirect
from dotenv import load_dotenv
from datetime import datetime, date, timedelta
import requests
import mysql.connector
import psycopg2
import psycopg2.extras
import os
import time
import threading
from pathlib import Path
import json
import re
import hmac
import hashlib
import base64
from urllib.parse import urlencode
from google import genai
from google.genai import types

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

# ── Autenticação (SSO vindo do Canaã Performance / BDR) ────────
# O OTDR fica atrás do proxy Nginx em /otdr/, mas não tinha login nenhum
# até aqui (qualquer um na rede via URL direta via acesso). O Canaã
# Performance (BDR) já autentica o usuário; quando ele clica no ícone do
# OTDR no menu, o backend do BDR gera um token assinado de curta duração
# (60s) e manda o navegador pra cá com ?sso=<token>. Validamos esse token
# aqui (mesma chave secreta, HMAC) e, se for válido, abrimos uma sessão
# própria do Flask (cookie assinado), sem precisar mexer em como o BDR
# autentica as próprias chamadas dele.
app.secret_key = os.getenv("FLASK_SECRET_KEY", "")
app.permanent_session_lifetime = timedelta(hours=8)  # mesma duração do JWT do BDR
OTDR_SSO_SECRET = os.getenv("OTDR_SSO_SECRET", "")

def _b64url_decode(s: str) -> bytes:
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))

def _validar_sso(token: str):
    """Verifica um token HS256 (mesmo formato gerado pelo jsonwebtoken do
    Node no backend do BDR), sem depender de nenhuma lib de JWT."""
    if not OTDR_SSO_SECRET:
        return None
    try:
        header_b64, payload_b64, sig_b64 = token.split(".")
        assinatura_esperada = hmac.new(
            OTDR_SSO_SECRET.encode(), f"{header_b64}.{payload_b64}".encode(), hashlib.sha256
        ).digest()
        assinatura_recebida = _b64url_decode(sig_b64)
        if not hmac.compare_digest(assinatura_esperada, assinatura_recebida):
            return None
        payload = json.loads(_b64url_decode(payload_b64))
        if payload.get("exp") and time.time() > payload["exp"]:
            return None
        return payload
    except Exception:
        return None

@app.before_request
def _checar_autenticacao():
    # Chamada interna de verdade (ex: otdr_alertas.py consultando
    # /api/saude_olt direto em 127.0.0.1) bate no Flask SEM passar pelo
    # Nginx, então não traz X-Real-IP/X-Forwarded-For (o Nginx é quem seta
    # esses headers no proxy de /otdr/). Checar só remote_addr não basta:
    # como o Nginx roda no mesmo servidor, QUALQUER requisição que passa por
    # ele também chega no Flask como 127.0.0.1 — sem o header, a exceção
    # abriria o gate pra todo mundo de fora também.
    veio_direto = request.remote_addr in ("127.0.0.1", "::1")
    tem_header_proxy = bool(request.headers.get("X-Real-IP") or request.headers.get("X-Forwarded-For"))
    if veio_direto and not tem_header_proxy:
        return None

    # Sem SSO_SECRET configurado, o gateway está desligado (comportamento
    # antigo, sem login) — evita travar o ambiente antes da configuração
    # estar pronta dos dois lados (BDR + OTDR).
    if not OTDR_SSO_SECRET:
        return None

    # Sessão já estabelecida vale sempre, ANTES de olhar pro "sso" — assim um
    # ?sso=... antigo/expirado que ainda esteja na URL (histórico, recarregar
    # a página) não derruba uma sessão que continua válida.
    if session.get("otdr_auth"):
        return None

    sso = request.args.get("sso")
    if sso:
        payload = _validar_sso(sso)
        if not payload:
            return "Link de acesso inválido ou expirado. Volte ao Canaã Performance e tente novamente.", 401
        session.clear()
        session["otdr_auth"] = True
        session["otdr_user"] = payload.get("nome", "desconhecido")
        session.permanent = True
        # Remove o "sso" da URL antes de continuar, pra não ficar no histórico.
        # request.path já vem SEM o prefixo /otdr (o Nginx tira antes de
        # repassar pro Flask) — esse trecho só roda em requisições que vieram
        # de fato pelo proxy público, então o prefixo pode ser fixo aqui.
        args = request.args.to_dict()
        args.pop("sso", None)
        qs = urlencode(args)
        return redirect(f"/otdr{request.path}" + (f"?{qs}" if qs else ""))

    return "Acesso restrito. Entre pelo Canaã Performance (hub.canaatelecom.com.br) e use o ícone do OTDR.", 401

# ── Fuso horário ───────────────────────────────────────────────
# O campo "latest_status_change" da API get_outage_pons já vem em horário
# local de Brasília (achávamos que era UTC, mas em 08/07/2026 confirmamos ao
# vivo que não é: o SmartOLT mostrava "X min atrás" batendo com o horário
# local, e o alerta batia com o horário real do evento só depois de
# subtrairmos 3h a mais por engano).
# "last_status_change" da API get_all_onus_details também já é local — nunca
# precisou de conversão.
from datetime import timezone
_TZ_BRASILIA = timezone(timedelta(hours=-3))

def _utc_para_brasilia(ts_str):
    if not ts_str:
        return ts_str
    try:
        dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return ts_str

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

# ── Cache cliente_id→dados (IXC) — fallback de identificação ──
# O SmartOLT grava "id<ID_cliente>" no campo name da ONU. Quando o cruzamento
# por MAC falha, usamos esse ID para recuperar o cliente (inclusive cancelados).
_cli_cache      = {"data": None, "timestamp": 0}
_cli_cache_lock = threading.Lock()

def get_cliente_map():
    agora = time.time()
    with _cli_cache_lock:
        if _cli_cache["data"] and (agora - _cli_cache["timestamp"]) < MAC_CACHE_TTL:
            return _cli_cache["data"]
    conn = None
    try:
        conn = get_mysql()
        cur  = conn.cursor(dictionary=True)
        cur.execute("SELECT id, razao, ativo FROM cliente")
        mapa = {int(r["id"]): {"id": r["id"], "nome": r["razao"],
                               "ativo": str(r.get("ativo")) == "S"} for r in cur.fetchall()}
        cur.close()
        with _cli_cache_lock:
            _cli_cache["data"] = mapa
            _cli_cache["timestamp"] = agora
        return mapa
    except Exception as e:
        app.logger.error(f"get_cliente_map falhou: {e}")
        with _cli_cache_lock:
            return _cli_cache["data"] or {}
    finally:
        if conn:
            try: conn.close()
            except: pass

def identificar_cliente(onu, mac_map, cliente_map):
    """Cruza a ONU com o cliente: 1º por MAC (login ativo), 2º por id<N> no name."""
    sn = (onu.get("sn") or "").upper()
    cli = mac_map.get(sn)
    if cli:
        return {"id": cli["id"], "nome": cli["nome"], "ativo": True, "via": "mac"}
    m = re.match(r"^id(\d+)$", str(onu.get("name", "")).strip(), re.I)
    if m:
        c = cliente_map.get(int(m.group(1)))
        if c:
            return {"id": c["id"], "nome": c["nome"], "ativo": c["ativo"], "via": "id"}
    return None

def montar_sn_name_map():
    """SN → campo 'name' do SmartOLT (cache atual), usado como fallback id<N>
    para identificar cliente em registros históricos (que não guardam 'name')."""
    onus_cache, _, _ = get_onus(force=False)
    return {(o.get("sn") or "").upper(): o.get("name") for o in onus_cache}

def nome_cliente_por_sn(sn, mac_map, cliente_map, sn_name_map):
    """Identifica o cliente de um SN histórico: 1º por MAC, 2º por id<N> via cache atual."""
    su = (sn or "").upper()
    cli = mac_map.get(su)
    if cli:
        return cli["nome"]
    m = re.match(r"^id(\d+)$", str(sn_name_map.get(su) or "").strip(), re.I)
    if m:
        c = cliente_map.get(int(m.group(1)))
        if c:
            return c["nome"]
    return ""

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
        idade     = agora - _cache["timestamp"]

    # Sem force → sempre retorna cache se existir (page load normal)
    if tem_cache and not force:
        with _cache_lock:
            return _cache["data"], False, False

    # force=True mas cache ainda fresco (< cooldown) → não bate na API.
    # Protege o rate limit do SmartOLT contra cliques repetidos no botão Atualizar.
    if tem_cache and force and idade < CACHE_TTL_MIN:
        with _cache_lock:
            return _cache["data"], False, False

    # force=True (fora do cooldown) ou sem cache → chama a API
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

# Mantida fora do menu principal (drill-down técnico a partir do Painel),
# não é mais item de navegação de topo.
@app.route("/os")
def os_page():  return render_template("os.html")

@app.route("/metricas")
def metricas_page(): return render_template("metricas.html")

# Compatibilidade com links/favoritos antigos.
@app.route("/kpis")
def kpis_page(): return redirect("/metricas")

@app.route("/historico")
def hist_page(): return render_template("historico.html")

# Absorvida pela aba "Histórico de alertas" dentro de Avisos de parada, no Painel.
@app.route("/alertas")
def alertas_page(): return redirect("/painel")

# Absorvida pelo drill-down de OLT no Painel — sem página própria.
@app.route("/saude")
def saude_page(): return redirect("/painel")

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

# ── SLA de chamados (tempo de permanência aberto) ──────────────
SLA_LIMIAR_ATENCAO = int(os.getenv("OTDR_SLA_ATENCAO_DIAS", "3"))
SLA_LIMIAR_CRITICO = int(os.getenv("OTDR_SLA_CRITICO_DIAS", "7"))

def _chamados_sla(top_n=10):
    """Tempo de permanência dos chamados em aberto (status 'A'/'AS', mesma
    definição usada em 'em_aberto'). Reaproveitada por /api/kpis e /api/painel."""
    conn = get_mysql()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(f"""
            SELECT
                COUNT(*) AS total_abertos,
                ROUND(AVG(DATEDIFF(NOW(), data_abertura)),1) AS media_dias,
                SUM(CASE WHEN DATEDIFF(NOW(), data_abertura) >= %s THEN 1 ELSE 0 END) AS represados_atencao,
                SUM(CASE WHEN DATEDIFF(NOW(), data_abertura) >= %s THEN 1 ELSE 0 END) AS represados_critico
            FROM su_oss_chamado
            WHERE status IN ('A','AS') AND id_assunto IN ({ASSUNTOS_STR})
        """, (SLA_LIMIAR_ATENCAO, SLA_LIMIAR_CRITICO))
        agg = cur.fetchone()

        cur.execute(f"""
            SELECT os.id AS id_atd, cli.razao AS nome, ass.assunto, pop.pop,
                   os.data_abertura, DATEDIFF(NOW(), os.data_abertura) AS dias_aberto
            FROM su_oss_chamado os
            LEFT JOIN su_oss_assunto ass ON os.id_assunto = ass.id
            LEFT JOIN cliente cli ON os.id_cliente = cli.id
            LEFT JOIN radusuarios usr ON usr.id_cliente = cli.id
            LEFT JOIN radpop_radio_cliente_fibra fibra ON fibra.id_login = usr.id
            LEFT JOIN radpop_radio radio ON fibra.id_transmissor = radio.id
            LEFT JOIN radpop pop ON radio.id_pop = pop.id
            WHERE os.status IN ('A','AS') AND os.id_assunto IN ({ASSUNTOS_STR})
            ORDER BY os.data_abertura ASC
            LIMIT %s
        """, (top_n,))
        mais_antigos = cur.fetchall()
        cur.close()
    finally:
        conn.close()

    for r in mais_antigos:
        if isinstance(r.get("data_abertura"), datetime):
            r["data_abertura"] = r["data_abertura"].strftime("%d/%m/%Y %H:%M")

    return {
        "total_abertos":      agg["total_abertos"] or 0,
        "media_dias":         float(agg["media_dias"]) if agg["media_dias"] is not None else 0,
        "represados_atencao": agg["represados_atencao"] or 0,
        "represados_critico": agg["represados_critico"] or 0,
        "limiares":           {"atencao": SLA_LIMIAR_ATENCAO, "critico": SLA_LIMIAR_CRITICO},
        "mais_antigos":       mais_antigos,
    }

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

        sla = _chamados_sla()

        return jsonify({
            "status":"ok",
            "timestamp":datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
            "rede":{"total_onus":t,"online":online,"offline":offline,
                    "pct_online":round(online/t*100,1) if t else 0,
                    "falha_sync":falha_sync,"desabilitadas":desabilitadas,**sinal},
            "os":{"abertos_hoje":abertos_hoje,"em_aberto":em_aberto,
                  "por_assunto":por_assunto,"por_pop":por_pop,"sla":sla},
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

@app.route("/api/historico/inicio")
def api_hist_inicio():
    """Data do snapshot mais antigo disponível no banco (independe do filtro
    de período aplicado na tela) — mostra desde quando o histórico existe."""
    try:
        conn = get_pg()
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("SELECT MIN(snapshot_data) primeiro, MAX(snapshot_data) ultimo, COUNT(DISTINCT snapshot_data) total_dias FROM otdr.historico_smartolt")
            r = cur.fetchone()
            cur.close()
        finally:
            conn.close()
        return jsonify({
            "status": "ok",
            "primeiro": r["primeiro"].strftime("%d/%m/%Y") if r["primeiro"] else None,
            "ultimo":   r["ultimo"].strftime("%d/%m/%Y") if r["ultimo"] else None,
            "total_dias": r["total_dias"] or 0,
        })
    except Exception as e:
        return jsonify({"status": "erro", "mensagem": str(e)}), 500

@app.route("/api/historico/alertas")
def api_historico_alertas():
    """Histórico de alertas de queda de porta: início, fim, causa e duração.
    Escrito pelo detector (otdr_alertas.py) em otdr.alertas_historico."""
    try:
        data_inicio = request.args.get("inicio", "").strip()
        data_fim    = request.args.get("fim", "").strip()
        olt         = request.args.get("olt", "").strip()
        status      = request.args.get("status", "").strip()  # "ativo" | "encerrado" | ""

        condicoes = []
        params: list = []
        if data_inicio:
            condicoes.append("inicio >= %s")
            params.append(data_inicio + " 00:00:00")
        if data_fim:
            condicoes.append("inicio <= %s")
            params.append(data_fim + " 23:59:59")
        if olt:
            condicoes.append("olt_nome ILIKE %s")
            params.append(f"%{olt}%")
        if status == "ativo":
            condicoes.append("fim IS NULL")
        elif status == "encerrado":
            condicoes.append("fim IS NOT NULL")

        where = ("WHERE " + " AND ".join(condicoes)) if condicoes else ""

        conn = get_pg()
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(f"""
                SELECT id, olt_id, olt_nome, board, porta, categoria, causa,
                       onus_afetados, onus_total, inicio, fim, duracao_segundos, synkr_notice_id
                FROM otdr.alertas_historico
                {where}
                ORDER BY inicio DESC
                LIMIT 500
            """, params)
            linhas = cur.fetchall()
            cur.close()
        finally:
            conn.close()

        resultados = [{
            "id":                r["id"],
            "olt_nome":          r["olt_nome"],
            "porta":             f"{r['board']}/{r['porta']}",
            "categoria":         r["categoria"],
            "causa":             r["causa"],
            "onus_afetados":     r["onus_afetados"],
            "onus_total":        r["onus_total"],
            "inicio":            r["inicio"].strftime("%Y-%m-%d %H:%M:%S") if r["inicio"] else None,
            "fim":               r["fim"].strftime("%Y-%m-%d %H:%M:%S") if r["fim"] else None,
            "duracao_segundos":  r["duracao_segundos"],
            "ativo":             r["fim"] is None,
            "synkr_notice_id":   r["synkr_notice_id"],
        } for r in linhas]

        return jsonify({"status": "ok", "alertas": resultados})
    except Exception as e:
        return jsonify({"status": "erro", "mensagem": str(e)}), 500

@app.route("/api/historico/detalhe")
def api_hist_detalhe():
    """Drill-down do gráfico de evolução: ONUs de um snapshot (dia) e nível,
    com cliente (cruzado do IXC), PON e sinal RX. Agrupável por OLT."""
    try:
        data_str = request.args.get("data", "").strip()
        nivel    = request.args.get("nivel", "").strip()  # ex: '1 - Atencao' (vazio = todos)
        if "/" in data_str:
            partes = data_str.split("/")
            if len(partes) == 3:
                d, m, y = partes
                data_iso = f"{y}-{m.zfill(2)}-{d.zfill(2)}"
            else:
                data_iso = data_str
        else:
            data_iso = data_str

        conn = get_pg()
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            where = "snapshot_data = %s"
            params = [data_iso]
            if nivel:
                where += " AND nivel_sinal = %s"
                params.append(nivel)
            cur.execute(f"""
                SELECT sn, olt_name, board, port, onu, sinal_rx, nivel_sinal
                FROM otdr.historico_smartolt
                WHERE {where}
                ORDER BY sinal_rx ASC
                LIMIT 3000
            """, params)
            rows = [dict(r) for r in cur.fetchall()]
            cur.close()
        finally:
            conn.close()

        # cruzamento cliente: MAC (login ativo) e fallback id<N> via cache atual
        mac_map = get_mac_map()
        cliente_map = get_cliente_map()
        sn_name = montar_sn_name_map()

        lista = []
        for r in rows:
            lista.append({
                "sn":      r["sn"],
                "olt":     r["olt_name"],
                "pon":     f"{r['board']}/{r['port']}/{r['onu']}",
                "cliente": nome_cliente_por_sn(r["sn"], mac_map, cliente_map, sn_name),
                "rx":      float(r["sinal_rx"]) if r["sinal_rx"] is not None else None,
                "nivel":   r["nivel_sinal"],
            })

        por_olt = {}
        for x in lista:
            por_olt[x["olt"]] = por_olt.get(x["olt"], 0) + 1
        resumo = sorted([{"olt": k, "qtd": v} for k, v in por_olt.items()], key=lambda x: -x["qtd"])
        return jsonify({"status": "ok", "data": data_str, "nivel": nivel,
                        "total": len(lista), "por_olt": resumo, "onus": lista})
    except Exception as e:
        return jsonify({"status": "erro", "mensagem": str(e)}), 500

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
    """ONUs com sinal ruim AGORA (ao vivo) — não eventos já resolvidos.
    Para cada uma, mostra também a maior queda registrada no período
    (quando aconteceu e quanto caiu), mantendo o contexto histórico."""
    try:
        dt_ini, dt_fim = _hist_datas()

        # 1. Status ATUAL (cache ao vivo) — é isso que define quem entra na lista
        onus_live, _, _ = get_onus(force=False)
        live_por_sn = {}
        for o in onus_live:
            sn = (o.get("sn") or "").upper()
            if not sn:
                continue
            rx_raw = o.get("signal_1310")
            try:
                rx = float(rx_raw) if rx_raw not in (None, "", "null") else None
            except (TypeError, ValueError):
                rx = None
            nivel = classificar(rx)
            if nivel in (NIVEL_NORMAL, NIVEL_SEM):
                continue  # só entra quem está degradado agora
            live_por_sn[sn] = {
                "rx_hoje": rx, "nivel_hoje": nivel, "status_hoje": o.get("status"),
                "olt_name": o.get("olt_name"), "onu_type": o.get("onu_type_name"),
                "pon": f"{o.get('board')}/{o.get('port')}/{o.get('onu')}",
            }

        if not live_por_sn:
            return jsonify({"status": "ok", "dados": []})

        # 2. Maior queda registrada no histórico do período, por SN (contexto)
        conn = get_pg()
        try:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("""
                WITH ranked AS (
                    SELECT
                        sn, snapshot_data, sinal_rx,
                        LAG(sinal_rx) OVER (PARTITION BY sn ORDER BY snapshot_data) AS rx_anterior,
                        LAG(snapshot_data) OVER (PARTITION BY sn ORDER BY snapshot_data) AS data_anterior
                    FROM otdr.historico_smartolt
                    WHERE snapshot_data BETWEEN %s AND %s
                ),
                quedas AS (
                    SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY sn ORDER BY (sinal_rx - rx_anterior) ASC) AS rn
                    FROM ranked
                    WHERE rx_anterior IS NOT NULL AND sinal_rx < rx_anterior
                )
                SELECT sn, snapshot_data AS data_queda, sinal_rx AS rx_na_queda,
                       data_anterior, rx_anterior
                FROM quedas WHERE rn = 1
            """, (dt_ini, dt_fim))
            quedas_por_sn = {r["sn"]: dict(r) for r in cur.fetchall()}
            cur.close()
        finally:
            conn.close()

        # 3. Combina: status de hoje (obrigatório) + maior queda do período (se houver)
        mac_map = get_mac_map()
        cliente_map = get_cliente_map()
        sn_name = montar_sn_name_map()
        resultado = []
        for sn, live in live_por_sn.items():
            queda = quedas_por_sn.get(sn)
            item = {
                "sn": sn, **live,
                "cliente": nome_cliente_por_sn(sn, mac_map, cliente_map, sn_name),
                "data_queda": None, "rx_na_queda": None,
                "data_anterior": None, "rx_anterior": None,
            }
            if queda:
                item["data_queda"] = queda["data_queda"].strftime("%d/%m/%Y") if queda.get("data_queda") else None
                item["rx_na_queda"] = float(queda["rx_na_queda"]) if queda.get("rx_na_queda") is not None else None
                item["data_anterior"] = queda["data_anterior"].strftime("%d/%m/%Y") if queda.get("data_anterior") else None
                item["rx_anterior"] = float(queda["rx_anterior"]) if queda.get("rx_anterior") is not None else None
            resultado.append(item)

        # pior sinal de hoje primeiro (mais urgente no topo)
        resultado.sort(key=lambda x: x["rx_hoje"] if x["rx_hoje"] is not None else 0)
        return jsonify({"status": "ok", "dados": resultado[:150]})
    except Exception as e:
        return jsonify({"status": "erro", "mensagem": str(e)}), 500

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
                    sn, olt_name, onu_type,
                    COUNT(*) AS dias_degradado,
                    MIN(sinal_rx) AS pior_rx,
                    ROUND(AVG(sinal_rx)::numeric,2) AS media_rx,
                    MAX(snapshot_data) AS ultima_data,
                    MIN(snapshot_data) AS primeira_data,
                    MODE() WITHIN GROUP (ORDER BY nivel_sinal) AS nivel_predominante,
                    (array_agg(board ORDER BY snapshot_data DESC))[1] AS board,
                    (array_agg(port  ORDER BY snapshot_data DESC))[1] AS port,
                    (array_agg(onu   ORDER BY snapshot_data DESC))[1] AS onu
                FROM otdr.historico_smartolt
                WHERE snapshot_data BETWEEN %s AND %s
                GROUP BY sn, olt_name, onu_type
                HAVING COUNT(*) > 1
                ORDER BY dias_degradado DESC, pior_rx ASC
                LIMIT 200
            """, (dt_ini, dt_fim))
            rows = [dict(r) for r in cur.fetchall()]
            cur.close()
        finally:
            conn.close()
        mac_map = get_mac_map()
        cliente_map = get_cliente_map()
        sn_name = montar_sn_name_map()
        for r in rows:
            for k in ("ultima_data","primeira_data"):
                if r.get(k): r[k] = r[k].strftime("%d/%m/%Y")
            for k in ("pior_rx","media_rx"):
                if r.get(k) is not None: r[k] = float(r[k])
            r["pon"] = f"{r.get('board')}/{r.get('port')}/{r.get('onu')}"
            r["cliente"] = nome_cliente_por_sn(r.get("sn"), mac_map, cliente_map, sn_name)
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

# Categoria = seção do get_outage_pons onde a porta apareceu. É isso que
# distingue LOS parcial de LOS total, power fail e offline — não existe campo
# "outage_cause"/"cause" na resposta real da API (mito de versões antigas).
CAUSAS_MAP = {
    "partial_los": "LOS parcial",
    "los":         "LOS total (fibra cortada)",
    "power":       "Power Fail",
    "offline":     "Offline não identificado",
}

def _buscar_outages():
    """Busca única e global de outages de PON (com cache de 2min).
    Reaproveitada por /api/alertas/status e /api/painel."""
    global _alertas_cache
    agora = time.time()
    if _alertas_cache["data"] is not None and (agora - _alertas_cache["ts"]) < _ALERTAS_TTL:
        d = dict(_alertas_cache["data"])
        d["cached"] = True
        return d

    # Consulta única e global — a resposta real é
    # {"response": {"sections": [{"key": "partial_los"|"los"|"power"|"offline",
    # "groups": [{"pons": [{board, port, olt_id, olt_name, los_count, ...}]}]}]}}
    r = requests.get(f"{SMARTOLT_URL}/api/system/get_outage_pons/", headers=HEADERS, timeout=15)
    r.raise_for_status()
    d = r.json()
    sections = (d.get("response") or {}).get("sections", [])

    # Categorias fixas (sempre presentes no resumo, mesmo com 0 ocorrências,
    # para o frontend poder mostrar o "✓ ok" quando não há problema)
    CATEGORIAS_ORDEM = ["partial_los", "los", "power", "offline"]
    resumo = {c: {"pons": 0, "onus": 0} for c in CATEGORIAS_ORDEM}

    outages = []
    for sec in sections:
        categoria = sec.get("key", "")
        if categoria in resumo:
            resumo[categoria] = {"pons": sec.get("pon_count", 0), "onus": sec.get("subscribers", 0)}
        for grupo in sec.get("groups", []):
            for p in grupo.get("pons", []):
                total_onus = p.get("total_onus")
                afetados = p.get("affected_onus")
                afetados_pct = p.get("affected_percent")
                # Outage TOTAL (los/power/offline): a própria porta inteira caiu,
                # então mesmo sem o campo vir preenchido pelo SmartOLT, o
                # afetado real é a porta inteira (100%). Só "partial_los" tem
                # um valor parcial de verdade vindo da API.
                if afetados is None and categoria != "partial_los":
                    afetados = total_onus
                    afetados_pct = 100
                outages.append({
                    "olt_id":       p.get("olt_id"),
                    "olt_nome":     p.get("olt_name"),
                    "board":        p.get("board"),
                    "port":         p.get("port"),
                    "onus":         total_onus,
                    "afetados":     afetados,
                    "afetados_pct": afetados_pct,
                    "los":          p.get("los_count", 0),
                    "pwrfail":      p.get("power_count", 0),
                    "categoria":    categoria,
                    "causa":        CAUSAS_MAP.get(categoria, "Desconhecida"),
                    "desde":        _utc_para_brasilia(p.get("latest_status_change", "")),
                })

    resultado = {
        "status":  "ok",
        "outages": outages,
        "resumo":  resumo,
        "total":   len(outages),
        "ts":      datetime.now().strftime("%H:%M"),
        "cached":  False,
    }
    _alertas_cache = {"data": resultado, "ts": agora}
    return resultado

@app.route("/api/alertas/status")
def api_alertas_status():
    try:
        return jsonify(_buscar_outages())
    except Exception as e:
        return jsonify({"status": "error", "mensagem": str(e), "outages": [], "resumo": {}, "total": 0}), 500

@app.route("/api/pon_outage/onus")
def api_pon_outage_onus():
    """Drill-down do widget PON outage: quais ONUs específicas estão afetadas
    numa porta (olt_id + board + port), cruzadas com o cliente (IXC).
    Não bate na API do SmartOLT — usa o cache ao vivo já mantido pelo sistema."""
    try:
        olt_id = request.args.get("olt_id", "").strip()
        board  = request.args.get("board", "").strip()
        port   = request.args.get("port", "").strip()
        if not (olt_id and board and port):
            return jsonify({"status": "erro", "mensagem": "olt_id, board e port são obrigatórios."}), 400

        onus_cache, _, _ = get_onus(force=False)
        mac_map = get_mac_map()
        cliente_map = get_cliente_map()

        lista = []
        for o in onus_cache:
            if str(o.get("olt_id", "")) != olt_id: continue
            if str(o.get("board", "")) != board: continue
            if str(o.get("port", "")) != port: continue
            cli = identificar_cliente(o, mac_map, cliente_map)
            rx_raw = o.get("signal_1310")
            try:
                rx = float(rx_raw) if rx_raw not in (None, "", "null") else None
            except (TypeError, ValueError):
                rx = None
            lista.append({
                "sn":             o.get("sn"),
                "onu":            o.get("onu"),
                "status":         o.get("status"),
                "rx":             rx,
                "cliente":        cli["nome"] if cli else "",
                "cliente_ativo":  cli["ativo"] if cli else None,
                "endereco":       (o.get("address") or "").strip(),
                "ultima_mudanca": o.get("last_status_change"),
            })

        # problemáticas primeiro (não online), depois por gravidade
        ordem_status = {"LOS": 0, "Power fail": 1, "Offline": 2}
        lista.sort(key=lambda x: (0 if x["status"] != "Online" else 1, ordem_status.get(x["status"], 9)))

        return jsonify({
            "status": "ok", "olt_id": olt_id, "board": board, "port": port,
            "total": len(lista), "onus": lista,
        })
    except Exception as e:
        return jsonify({"status": "erro", "mensagem": str(e)}), 500


# ── Saúde por OLT (do cache — sem chamada extra à API) ────────
SAUDE_ATENCAO = float(os.getenv("OTDR_SAUDE_ATENCAO", "10"))  # % offline
SAUDE_CRITICO = float(os.getenv("OTDR_SAUDE_CRITICO", "18"))  # % offline
SAUDE_ESTADO_FILE = BASE_DIR / "saude_estado.json"

# ── SYNKR (Aprimorar) — atualização manual de avisos de parada ─
# O detector (otdr_alertas.py) cria e fecha os avisos automaticamente.
# Aqui só permitimos ATUALIZAR prazo/descrição de um aviso já aberto, sem
# fechá-lo — para quando o time de campo avalia o problema no local e o
# prazo genérico de 4h precisa ser ajustado (maior ou menor).
SYNKR_URL       = os.getenv("SYNKR_URL", "https://synkr.aprimorar.net.br/api").rstrip("/")
SYNKR_LOGIN     = os.getenv("SYNKR_LOGIN", "")
SYNKR_PASSWORD  = os.getenv("SYNKR_PASSWORD", "")
SYNKR_BUSINESS  = os.getenv("SYNKR_BUSINESS_NAME", "CANAA")
SYNKR_AVISOS_FILE = BASE_DIR / "synkr_avisos.json"

def _ler_synkr_avisos():
    """Avisos abertos, escritos pelo detector (otdr_alertas.py): chave → notice_id."""
    try:
        if SYNKR_AVISOS_FILE.exists():
            with open(SYNKR_AVISOS_FILE) as f:
                return json.load(f)
    except Exception:
        pass
    return {}

def _synkr_autenticar():
    if not (SYNKR_LOGIN and SYNKR_PASSWORD):
        return None
    try:
        r = requests.post(f"{SYNKR_URL}/auth/sign_in",
                           json={"login": SYNKR_LOGIN, "password": SYNKR_PASSWORD}, timeout=15)
        r.raise_for_status()
        token = r.json().get("token")
        if not token:
            return None
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        r = requests.get(f"{SYNKR_URL}/me/business", headers=headers, timeout=15)
        r.raise_for_status()
        negocios = r.json().get("data", [])
        alvo = next((n for n in negocios if n.get("name") == SYNKR_BUSINESS), None)
        if not alvo:
            return None
        r = requests.post(f"{SYNKR_URL}/me/business", headers=headers, json={"id": alvo["id"]}, timeout=15)
        r.raise_for_status()
        return token
    except requests.exceptions.RequestException:
        return None

@app.route("/api/synkr/avisos")
def api_synkr_avisos():
    """Lista os avisos de parada atualmente abertos no SYNKR, com detalhes
    ao vivo (descrição, prazo, status), para o NOC acompanhar e atualizar."""
    try:
        avisos_map = _ler_synkr_avisos()
        configurado = bool(SYNKR_LOGIN and SYNKR_PASSWORD)
        if not avisos_map or not configurado:
            return jsonify({"status": "ok", "avisos": [], "configurado": configurado})

        token = _synkr_autenticar()
        if not token:
            return jsonify({"status": "ok", "avisos": [], "configurado": False,
                             "mensagem": "Falha na autenticação com o SYNKR."})

        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        resultado = []
        for chave, notice_id in avisos_map.items():
            try:
                r = requests.get(f"{SYNKR_URL}/callcenter/notice/{notice_id}", headers=headers, timeout=15)
                r.raise_for_status()
                d = r.json()
                resultado.append({
                    "chave": chave, "notice_id": notice_id,
                    "status": d.get("status"), "description": d.get("description"),
                    "start_date": d.get("start_date"), "deadline": d.get("deadline"),
                    "responsible_name": d.get("responsible_name"),
                })
            except Exception as e:
                resultado.append({"chave": chave, "notice_id": notice_id, "erro": str(e)})
        return jsonify({"status": "ok", "avisos": resultado, "configurado": True})
    except Exception as e:
        return jsonify({"status": "erro", "mensagem": str(e)}), 500

@app.route("/api/synkr/atualizar", methods=["POST"])
def api_synkr_atualizar():
    """Atualiza o prazo/descrição de um aviso já aberto, SEM encerrá-lo."""
    try:
        dados = request.get_json(force=True) or {}
        chave    = (dados.get("chave") or "").strip()
        deadline = (dados.get("deadline") or "").strip()
        if not (chave and deadline):
            return jsonify({"status": "erro", "mensagem": "chave e deadline são obrigatórios."}), 400
        if len(deadline) == 16:  # "YYYY-MM-DDTHH:MM" (input datetime-local, sem segundos)
            deadline += ":00"

        avisos_map = _ler_synkr_avisos()
        notice_id = avisos_map.get(chave)
        if notice_id is None:
            return jsonify({"status": "erro", "mensagem": f"Nenhum aviso aberto encontrado para '{chave}'."}), 404

        token = _synkr_autenticar()
        if not token:
            return jsonify({"status": "erro", "mensagem": "Falha na autenticação com o SYNKR."}), 502

        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        body = {
            "description": (dados.get("description") or "").strip() or "Prazo atualizado após avaliação em campo.",
            "deadline": deadline,
            "impact": (dados.get("impact") or "").strip(),
            "text_for_client": (dados.get("text_for_client") or "").strip() or "Nossa equipe está atuando na correção.",
            "finished": False,
        }
        r = requests.post(f"{SYNKR_URL}/callcenter/notice/{notice_id}/report",
                           headers=headers, json=body, timeout=15)
        r.raise_for_status()
        return jsonify({"status": "ok", "notice_id": notice_id})
    except requests.exceptions.RequestException as e:
        return jsonify({"status": "erro", "mensagem": str(e)}), 502
    except Exception as e:
        return jsonify({"status": "erro", "mensagem": str(e)}), 500

@app.route("/api/synkr/encerrar", methods=["POST"])
def api_synkr_encerrar():
    """Encerra manualmente um aviso aberto — rede de segurança para quando a
    detecção automática de recuperação (otdr_alertas.py) não pegar o caso
    (ex: SmartOLT fora do ar, OLT renomeada/removida)."""
    try:
        dados = request.get_json(force=True) or {}
        chave = (dados.get("chave") or "").strip()
        if not chave:
            return jsonify({"status": "erro", "mensagem": "chave é obrigatória."}), 400

        avisos_map = _ler_synkr_avisos()
        notice_id = avisos_map.get(chave)
        if notice_id is None:
            return jsonify({"status": "erro", "mensagem": f"Nenhum aviso aberto encontrado para '{chave}'."}), 404

        token = _synkr_autenticar()
        if not token:
            return jsonify({"status": "erro", "mensagem": "Falha na autenticação com o SYNKR."}), 502

        motivo = (dados.get("motivo") or "").strip() or "Encerrado manualmente pelo NOC."
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        body = {
            "description": motivo,
            "deadline": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "impact": "Encerrado manualmente.",
            "text_for_client": "O problema foi normalizado. Pedimos desculpas pelo transtorno.",
            "finished": True,
        }
        r = requests.post(f"{SYNKR_URL}/callcenter/notice/{notice_id}/report",
                           headers=headers, json=body, timeout=15)
        r.raise_for_status()

        # Remove do rastreio local (relendo o arquivo antes de gravar, pra não
        # sobrescrever uma mudança feita nesse meio tempo pelo detector).
        try:
            avisos_atual = _ler_synkr_avisos()
            avisos_atual.pop(chave, None)
            with open(SYNKR_AVISOS_FILE, "w") as f:
                json.dump(avisos_atual, f)
        except Exception as e:
            app.logger.warning(f"[SYNKR] Falha ao atualizar arquivo de avisos após encerrar: {e}")

        return jsonify({"status": "ok", "notice_id": notice_id})
    except requests.exceptions.RequestException as e:
        return jsonify({"status": "erro", "mensagem": str(e)}), 502
    except Exception as e:
        return jsonify({"status": "erro", "mensagem": str(e)}), 500

# ── IA (Gemini) — causa provável dos avisos SYNKR ──────────────
# Sugestão para o NOC revisar antes de repassar ao cliente/Aprimorar, não
# substitui a descrição enviada automaticamente (mesma filosofia do
# "Encerrar agora": IA sugere, humano decide).
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL   = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
_gemini_client = None

def _gemini():
    global _gemini_client
    if _gemini_client is None and GEMINI_API_KEY:
        _gemini_client = genai.Client(api_key=GEMINI_API_KEY)
    return _gemini_client

_CAUSA_PROVAVEL_PROMPT = """Você é um analista de rede FTTH/PON sênior do NOC da Canaã Telecom.
Com base SOMENTE nos dados fornecidos abaixo, escreva uma explicação curta
(2 a 4 frases) da causa provável da interrupção.

Regras:
- Não invente informação que não está nos dados.
- Se os dados não permitirem determinar a causa com confiança, diga isso
  explicitamente ao invés de especular.
- Não use travessão em nenhuma frase.
- Seja direto e técnico, sem saudação nem introdução."""

def _causa_provavel_contexto(chave: str):
    """Monta o contexto textual com dados reais para uma chave de aviso SYNKR.
    Chave é 'olt_id:board:port' (outage de porta) ou 'saude:olt_nome' (saúde de OLT)."""
    if chave.startswith("saude:"):
        olt_nome = chave.split(":", 1)[1]
        olts, _, _ = _calcular_saude_olts()
        olt = next((o for o in olts if o["olt"] == olt_nome), None)
        if not olt:
            return None
        return "\n".join([
            f"OLT: {olt_nome}",
            f"Percentual de clientes ativos offline: {olt['pct_offline']}%",
            f"Total de clientes ativos monitorados: {olt['total']}",
            f"Offline por LOS (sem sinal): {olt['los']}",
            f"Offline por queda de energia: {olt['power_fail']}",
            f"Offline sem causa identificada: {olt['offline_puro']}",
            f"Nível de saúde: {olt['nivel']}",
        ])

    partes = chave.split(":")
    if len(partes) != 3:
        return None
    olt_id, board, port = partes
    outages = _buscar_outages().get("outages", [])
    outage = next((o for o in outages
                   if str(o.get("olt_id")) == olt_id
                   and str(o.get("board")) == board
                   and str(o.get("port")) == port), None)
    if not outage:
        return None

    # force=True (com cooldown de 5min já embutido em get_onus) — essa análise
    # é sob demanda pra um incidente ativo específico, não pode confiar num
    # cache sem limite de idade (force=False já causou uma causa provável
    # errada, baseada em ONUs que pareciam Online mas eram dado velho de
    # antes da queda começar).
    onus_cache, _, _ = get_onus(force=True)
    onus_porta = [o for o in onus_cache
                  if str(o.get("olt_id", "")) == olt_id
                  and str(o.get("board", "")) == board
                  and str(o.get("port", "")) == port]
    status_count: dict[str, int] = {}
    horarios = set()
    for o in onus_porta:
        st = o.get("status") or "?"
        status_count[st] = status_count.get(st, 0) + 1
        ts = o.get("last_status_change")
        if ts:
            horarios.add(ts)

    padrao = ("todas as ONUs afetadas mudaram de status praticamente ao mesmo tempo, "
              "sugerindo um evento único e simultâneo (ex: corte de fibra, queda de energia)"
              if len(horarios) <= 1 else
              "as ONUs afetadas mudaram de status em momentos diferentes entre si, "
              "sugerindo degradação progressiva ao invés de um evento único")

    return "\n".join([
        f"OLT: {outage['olt_nome']} (board {board}, porta {port})",
        f"Categoria detectada pelo SmartOLT: {outage['causa']}",
        f"Total de ONUs na porta: {outage['onus']}",
        f"ONUs afetadas: {outage['afetados']} ({outage['afetados_pct']}%)",
        f"Contagem de ONUs por status atual: {status_count}",
        f"Interrupção detectada desde: {outage['desde']}",
        f"Padrão temporal: {padrao}",
    ])

@app.route("/api/synkr/causa_provavel", methods=["POST"])
def api_synkr_causa_provavel():
    try:
        dados = request.get_json(force=True) or {}
        chave = (dados.get("chave") or "").strip()
        if not chave:
            return jsonify({"status": "erro", "mensagem": "chave é obrigatória."}), 400

        cliente = _gemini()
        if not cliente:
            return jsonify({"status": "erro", "mensagem": "IA não configurada (GEMINI_API_KEY ausente)."}), 503

        contexto = _causa_provavel_contexto(chave)
        if not contexto:
            return jsonify({"status": "erro",
                             "mensagem": "Dados insuficientes para essa chave (o problema pode já ter sido normalizado)."}), 404

        resposta = cliente.models.generate_content(
            model=GEMINI_MODEL,
            contents=contexto,
            config=types.GenerateContentConfig(
                system_instruction=_CAUSA_PROVAVEL_PROMPT,
                temperature=0.2,
                max_output_tokens=250,
                thinking_config=types.ThinkingConfig(thinking_budget=0),
            ),
        )
        texto = (resposta.text or "").strip()
        if not texto:
            return jsonify({"status": "erro", "mensagem": "IA não retornou resposta."}), 502
        return jsonify({"status": "ok", "causa": texto})
    except Exception as e:
        app.logger.error(f"[IA] Falha ao gerar causa provável: {e}")
        return jsonify({"status": "erro", "mensagem": "Falha ao consultar IA. Tente novamente."}), 500

def _nivel_saude(pct):
    if pct >= SAUDE_CRITICO: return "critico"
    if pct >= SAUDE_ATENCAO: return "atencao"
    return "ok"

def _ler_saude_estado():
    """Estado de alerta escrito pelo detector (otdr_alertas.py): critico_desde por OLT."""
    try:
        if SAUDE_ESTADO_FILE.exists():
            with open(SAUDE_ESTADO_FILE) as f:
                return json.load(f)
    except Exception:
        pass
    return {}

def _calcular_saude_olts():
    """Saúde por OLT medida sobre CLIENTES ATIVOS (exclui ONUs canceladas/órfãs,
    que são lixo de cadastro e não refletem problema de rede).
    Reaproveitada por /api/saude_olt e /api/painel."""
    onus, atualizado, _rl = get_onus(force=False)
    mac_map = get_mac_map()
    cliente_map = get_cliente_map()
    estado = _ler_saude_estado()
    agg = {}
    for o in onus:
        olt = o.get("olt_name") or "—"
        a = agg.setdefault(olt, {"total": 0, "online": 0, "offline": 0,
                                 "power_fail": 0, "los": 0, "offline_puro": 0,
                                 "cancelados": 0, "orfaos": 0})
        cli = identificar_cliente(o, mac_map, cliente_map)
        online = (o.get("status") == "Online")
        if cli and not cli["ativo"]:
            a["cancelados"] += 1          # cliente cancelado (lixo)
            continue
        if not cli:
            a["orfaos"] += 1              # sem vínculo (lixo)
            continue
        # cliente ativo
        a["total"] += 1
        if online:
            a["online"] += 1
        else:
            a["offline"] += 1
            st = o.get("status")
            if   st == "Power fail": a["power_fail"]   += 1
            elif st == "LOS":        a["los"]          += 1
            elif st == "Offline":    a["offline_puro"] += 1

    olts = []
    for olt, a in agg.items():
        pct = (a["offline"] / a["total"] * 100) if a["total"] else 0
        est = estado.get(olt, {})
        olts.append({**a, "olt": olt, "pct_offline": round(pct, 1),
                     "nivel": _nivel_saude(pct),
                     "lixo": a["cancelados"] + a["orfaos"],
                     "critico_desde": est.get("critico_desde")})
    olts.sort(key=lambda x: -x["pct_offline"])

    resumo = {
        "ok":      sum(1 for o in olts if o["nivel"] == "ok"),
        "atencao": sum(1 for o in olts if o["nivel"] == "atencao"),
        "critico": sum(1 for o in olts if o["nivel"] == "critico"),
        "total_olts": len(olts),
    }
    return olts, resumo, atualizado

@app.route("/api/saude_olt")
def api_saude_olt():
    try:
        olts, resumo, atualizado = _calcular_saude_olts()
        return jsonify({
            "status": "ok", "olts": olts, "resumo": resumo,
            "limiares": {"atencao": SAUDE_ATENCAO, "critico": SAUDE_CRITICO},
            "atualizado": atualizado,
            "timestamp": time.strftime("%d/%m/%Y %H:%M", time.localtime(_cache["timestamp"])),
        })
    except Exception as e:
        return jsonify({"status": "erro", "mensagem": str(e)}), 500

@app.route("/api/saude_olt/offline")
def api_saude_olt_offline():
    """Lista de ONUs offline de uma OLT, com nome do cliente (IXC)."""
    try:
        olt_alvo = request.args.get("olt", "")
        onus, _, _ = get_onus(force=False)
        mac_map = get_mac_map()
        cliente_map = get_cliente_map()
        lista = []
        for o in onus:
            if (o.get("olt_name") or "—") != olt_alvo:
                continue
            if o.get("status") == "Online":
                continue
            cli = identificar_cliente(o, mac_map, cliente_map)
            lista.append({
                "sn":            o.get("sn"),
                "porta":         f"{o.get('board')}/{o.get('port')}/{o.get('onu')}",
                "status":        o.get("status"),
                "zona":          o.get("zone_name"),
                "cliente_nome":  cli["nome"] if cli else "",
                "cliente_id":    cli["id"]   if cli else "",
                "identificado":  bool(cli),
                "cliente_ativo": cli["ativo"] if cli else None,
                "ultima_mudanca": o.get("last_status_change"),
            })
        # Ordena: clientes ATIVOS identificados primeiro (acionáveis), depois
        # cancelados (com nome), depois sem vínculo. Dentro, por gravidade e nome.
        ordem = {"Power fail": 0, "LOS": 1, "Offline": 2}
        def rank(x):
            if x["identificado"] and x["cliente_ativo"]:  g = 0
            elif x["identificado"]:                       g = 1  # cancelado, mas com nome
            else:                                         g = 2  # sem vínculo
            return (g, ordem.get(x["status"], 9), x["cliente_nome"] or "zzz")
        lista.sort(key=rank)
        ativos     = sum(1 for x in lista if x["identificado"] and x["cliente_ativo"])
        cancelados = sum(1 for x in lista if x["identificado"] and not x["cliente_ativo"])
        sem_vinc   = sum(1 for x in lista if not x["identificado"])
        return jsonify({
            "status": "ok", "olt": olt_alvo, "total": len(lista),
            "ativos": ativos, "cancelados": cancelados, "sem_vinculo": sem_vinc,
            "onus": lista,
        })
    except Exception as e:
        return jsonify({"status": "erro", "mensagem": str(e)}), 500

@app.route("/api/saude_olt/limpeza")
def api_saude_limpeza():
    """Lista de ONUs canceladas/órfãs (lixo de cadastro) de todas as OLTs —
    para a equipe de cadastro remover do SmartOLT e liberar slots de porta."""
    try:
        onus, _, _ = get_onus(force=False)
        mac_map = get_mac_map()
        cliente_map = get_cliente_map()
        lista = []
        for o in onus:
            cli = identificar_cliente(o, mac_map, cliente_map)
            if cli and cli["ativo"]:
                continue  # cliente ativo → não é lixo
            tipo = "Cancelado" if cli else "Sem vínculo"
            lista.append({
                "olt":          o.get("olt_name") or "—",
                "porta":        f"{o.get('board')}/{o.get('port')}/{o.get('onu')}",
                "sn":           o.get("sn"),
                "ex_cliente":   cli["nome"] if cli else "",
                "cliente_id":   cli["id"]   if cli else "",
                "tipo":         tipo,
                "status_onu":   o.get("status"),
            })
        lista.sort(key=lambda x: (x["olt"], x["porta"]))
        cancelados = sum(1 for x in lista if x["tipo"] == "Cancelado")
        return jsonify({
            "status": "ok", "total": len(lista),
            "cancelados": cancelados, "sem_vinculo": len(lista) - cancelados,
            "onus": lista,
        })
    except Exception as e:
        return jsonify({"status": "erro", "mensagem": str(e)}), 500

@app.route("/api/saude_olt/portas")
def api_saude_portas():
    """Ocupação das portas PON de uma OLT: ativos vs lixo vs limite do split.
    Ajuda o time de expansão a ver saturação real (descontando lixo)."""
    try:
        olt_alvo = request.args.get("olt", "")
        limite = int(os.getenv("OTDR_SPLIT_LIMITE", "128"))  # ONUs por porta (GPON 1:128)
        onus, _, _ = get_onus(force=False)
        mac_map = get_mac_map()
        cliente_map = get_cliente_map()
        portas = {}
        for o in onus:
            if (o.get("olt_name") or "—") != olt_alvo:
                continue
            chave = f"{o.get('board')}/{o.get('port')}"
            p = portas.setdefault(chave, {"porta": chave, "total": 0, "ativos": 0, "lixo": 0})
            p["total"] += 1
            cli = identificar_cliente(o, mac_map, cliente_map)
            if cli and cli["ativo"]:
                p["ativos"] += 1
            else:
                p["lixo"] += 1
        lista = []
        for p in portas.values():
            p["livre"] = max(limite - p["total"], 0)
            p["livre_limpo"] = max(limite - p["ativos"], 0)  # espaço se remover o lixo
            p["ocupacao"] = round(p["total"] / limite * 100, 1) if limite else 0
            lista.append(p)
        lista.sort(key=lambda x: -x["total"])
        return jsonify({
            "status": "ok", "olt": olt_alvo, "limite": limite,
            "total_portas": len(lista), "portas": lista,
        })
    except Exception as e:
        return jsonify({"status": "erro", "mensagem": str(e)}), 500


@app.route("/consulta")
def consulta_page(): return render_template("consulta.html")

@app.route("/api/consulta_cliente")
def api_consulta_cliente():
    """Consulta de cliente: roda as 4 checagens que o gestor faria manualmente
    (sinal ao vivo, histórico recorrente, porta compartilhada, saúde da OLT) e
    devolve um veredito direto. Não bate na API do SmartOLT — usa cache + PG.
    Aceita nome, CPF/CNPJ, ou SN direto (para ONU sem vínculo com o cadastro
    IXC, quando o processo de instalação falhou em vincular o cliente)."""
    try:
        termo = request.args.get("nome", "").strip()
        if len(termo) < 3:
            return jsonify({"status": "erro", "mensagem": "Digite ao menos 3 letras/números."}), 400

        limpo = re.sub(r"[.\-/\s:]", "", termo)
        onus_cache, _, _ = get_onus(force=False)
        mac_map = get_mac_map()
        cliente_map = get_cliente_map()

        # ── 1. Encontra ONUs cujo cliente bate com o termo buscado ──────────
        # Detecta o tipo de busca pelo formato do termo: CPF/CNPJ (só dígitos,
        # 11 ou 14 chars), SN/MAC (sem espaço, alfanumérico com dígito, direto
        # no cache do SmartOLT — cobre ONU sem vínculo no IXC) ou nome (padrão).
        modo = "nome"
        encontrados = []  # lista de (onu, cli_ou_None)

        if limpo.isdigit() and len(limpo) in (11, 14):
            modo = "cpf"
            conn_ixc = get_mysql()
            cur_ixc = conn_ixc.cursor(dictionary=True)
            cur_ixc.execute("SELECT id FROM cliente WHERE cnpj_cpf = %s", (limpo,))
            ids_cliente = {int(r["id"]) for r in cur_ixc.fetchall()}
            cur_ixc.close(); conn_ixc.close()
            if ids_cliente:
                for o in onus_cache:
                    cli = identificar_cliente(o, mac_map, cliente_map)
                    if cli and cli.get("id") in ids_cliente:
                        encontrados.append((o, cli))

        elif " " not in termo and any(c.isdigit() for c in limpo) and len(limpo) >= 6:
            modo = "sn"
            termo_up = limpo.upper()
            for o in onus_cache:
                if termo_up in (o.get("sn") or "").upper():
                    cli = identificar_cliente(o, mac_map, cliente_map)
                    encontrados.append((o, cli))  # cli pode ser None (ONU órfã)

        else:
            termo_low = termo.lower()
            for o in onus_cache:
                cli = identificar_cliente(o, mac_map, cliente_map)
                if not cli or termo_low not in (cli["nome"] or "").lower():
                    continue
                encontrados.append((o, cli))

        if not encontrados:
            return jsonify({"status": "ok", "resultados": [], "modo_busca": modo})

        # PG: uma conexão só, reaproveitada para todos os matches
        conn = get_pg()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        resultados = []
        for o, cli in encontrados[:10]:  # limite de segurança
            sn = (o.get("sn") or "").upper()
            rx_raw = o.get("signal_1310")
            try:
                rx_hoje = float(rx_raw) if rx_raw not in (None, "", "null") else None
            except (TypeError, ValueError):
                rx_hoje = None
            nivel_hoje = classificar(rx_hoje)
            olt_id, olt_name = str(o.get("olt_id", "")), o.get("olt_name", "")
            board, port, onu_idx = str(o.get("board", "")), str(o.get("port", "")), o.get("onu")

            # ── 2. Degradação recorrente (últimos 30 dias) ──────────────────
            cur.execute("""
                SELECT COUNT(*) AS dias_degradado, MIN(sinal_rx) AS pior_rx,
                       ROUND(AVG(sinal_rx)::numeric,2) AS media_rx,
                       MAX(snapshot_data) AS ultima_data, MIN(snapshot_data) AS primeira_data
                FROM otdr.historico_smartolt
                WHERE sn = %s AND snapshot_data >= CURRENT_DATE - INTERVAL '30 days'
            """, (sn,))
            rec = cur.fetchone()
            recorrente = None
            if rec and rec["dias_degradado"]:
                recorrente = {
                    "dias_degradado": rec["dias_degradado"],
                    "pior_rx": float(rec["pior_rx"]) if rec["pior_rx"] is not None else None,
                    "media_rx": float(rec["media_rx"]) if rec["media_rx"] is not None else None,
                    "primeira_data": rec["primeira_data"].strftime("%d/%m/%Y") if rec["primeira_data"] else None,
                    "ultima_data": rec["ultima_data"].strftime("%d/%m/%Y") if rec["ultima_data"] else None,
                }

            # ── 3. Maior queda registrada (últimos 30 dias) ─────────────────
            cur.execute("""
                WITH ranked AS (
                    SELECT snapshot_data, sinal_rx,
                        LAG(sinal_rx) OVER (ORDER BY snapshot_data) AS rx_anterior,
                        LAG(snapshot_data) OVER (ORDER BY snapshot_data) AS data_anterior
                    FROM otdr.historico_smartolt
                    WHERE sn = %s AND snapshot_data >= CURRENT_DATE - INTERVAL '30 days'
                )
                SELECT * FROM ranked WHERE rx_anterior IS NOT NULL AND sinal_rx < rx_anterior
                ORDER BY (sinal_rx - rx_anterior) ASC LIMIT 1
            """, (sn,))
            q = cur.fetchone()
            piora = None
            if q:
                piora = {
                    "data_queda": q["snapshot_data"].strftime("%d/%m/%Y"),
                    "rx_na_queda": float(q["sinal_rx"]),
                    "data_anterior": q["data_anterior"].strftime("%d/%m/%Y"),
                    "rx_anterior": float(q["rx_anterior"]),
                }

            # ── 4. Porta compartilhada: outros clientes afetados agora ──────
            vizinhos = [
                v for v in onus_cache
                if str(v.get("olt_id", "")) == olt_id and str(v.get("board", "")) == board
                and str(v.get("port", "")) == port and (v.get("sn") or "").upper() != sn
            ]

            def _rx_do_vizinho(v):
                rx_raw = v.get("signal_1310")
                try:
                    return float(rx_raw) if rx_raw not in (None, "", "null") else None
                except (TypeError, ValueError):
                    return None

            def _vizinho_com_sinal_degradado(v):
                # Só conta como evidência de infraestrutura compartilhada
                # (fibra/splitter) quando o problema é ÓPTICO: LOS (perda
                # total de sinal) ou, se ainda online, nível Crítico/Fora de
                # Operação. "Power fail" (energia do vizinho, problema
                # domiciliar dele) e "Offline" genérico (causa não
                # identificada) não têm relação com fibra/splitter — contar
                # isso junto inflava outros_afetados e disparava "provável
                # problema de infraestrutura compartilhada" mesmo quando os
                # vizinhos só estavam sem energia (achado do usuário
                # 2026-08-06, caso RAILA SPINDOLA / contrato 19113: os 4
                # "afetados" eram todos Power fail, sem relação com a
                # atenuação real do cliente).
                status = v.get("status")
                if status == "LOS":
                    return True
                if status == "Online":
                    return classificar(_rx_do_vizinho(v)) in (NIVEL_CRITICO, NIVEL_FORA)
                return False

            vizinhos_afetados = [v for v in vizinhos if _vizinho_com_sinal_degradado(v)]
            porta_saude = {
                "total_onus": len(vizinhos) + 1,
                "outros_afetados": len(vizinhos_afetados),
                "exemplos": [
                    {"cliente": (identificar_cliente(v, mac_map, cliente_map) or {}).get("nome", ""),
                     "status": v.get("status"),
                     "nivel": classificar(_rx_do_vizinho(v)) if v.get("status") == "Online" else None}
                    for v in vizinhos_afetados[:5]
                ],
            }

            # ── 5. Saúde da OLT inteira (mesma métrica da tela SAÚDE) ───────
            olt_onus = [v for v in onus_cache if v.get("olt_name") == olt_name]
            olt_total = olt_online = olt_offline = 0
            for v in olt_onus:
                cv = identificar_cliente(v, mac_map, cliente_map)
                if not cv or not cv["ativo"]:
                    continue
                olt_total += 1
                if v.get("status") == "Online": olt_online += 1
                else: olt_offline += 1
            olt_pct = (olt_offline / olt_total * 100) if olt_total else 0
            olt_saude = {"pct_offline": round(olt_pct, 1), "nivel": _nivel_saude(olt_pct)}

            # ── Veredito ─────────────────────────────────────────────────────
            veredito, gravidade = _montar_veredito(o.get("status"), nivel_hoje, recorrente, porta_saude, olt_saude)
            if not cli:
                veredito = ("Sem vínculo com o cadastro do IXC (provável falha no processo de "
                             "instalação). ") + veredito

            if cli:
                cliente_nome, cliente_id, cliente_ativo = cli["nome"], cli.get("id"), cli["ativo"]
            else:
                cliente_nome = f"Sem vínculo IXC (nome no SmartOLT: {o.get('name') or 'não informado'})"
                cliente_id, cliente_ativo = None, None

            resultados.append({
                "cliente_nome": cliente_nome, "cliente_id": cliente_id, "cliente_ativo": cliente_ativo,
                "sem_vinculo": cli is None,
                "sn": o.get("sn"), "olt_name": olt_name, "pon": f"{board}/{port}/{onu_idx}",
                "endereco": (o.get("address") or "").strip(),
                "rx_hoje": rx_hoje, "nivel_hoje": nivel_hoje, "status_hoje": o.get("status"),
                "ultima_mudanca": o.get("last_status_change"),
                "recorrente": recorrente, "piora": piora,
                "porta_saude": porta_saude, "olt_saude": olt_saude,
                "veredito": veredito, "gravidade": gravidade,
            })

        cur.close(); conn.close()
        return jsonify({"status": "ok", "resultados": resultados, "modo_busca": modo})
    except Exception as e:
        return jsonify({"status": "erro", "mensagem": str(e)}), 500

# ── IA (Gemini) — causa provável na Consulta de Cliente ────────
# Reaproveita os dados que a própria /api/consulta_cliente já calculou (o
# frontend manda de volta o resultado que está na tela) — não bate de novo
# no IXC/PG/SmartOLT, só formata e manda pro modelo. Isso deixa a consulta
# precisa (mesmos dados que o humano está vendo) e barata em tokens.
#
# "Trava para dedo nervoso": cache de 10min por SN (clique repetido no mesmo
# cliente reusa a resposta, sem gastar token de novo) + trava de concorrência
# (2 cliques quase simultâneos no mesmo SN não disparam 2 chamadas à IA).
_CONSULTA_CAUSA_CACHE: dict[str, dict] = {}
_CONSULTA_CAUSA_TTL = 600  # 10 min
_CONSULTA_CAUSA_TTL_CURTO = 60  # 1 min — quando o detalhe ao vivo da OLT não veio
_CONSULTA_CAUSA_EM_ANDAMENTO: set[str] = set()

_CAUSA_CLIENTE_PROMPT = """Você é um analista de rede FTTH/PON sênior do NOC da Canaã Telecom.
Com base SOMENTE nos dados fornecidos abaixo sobre um cliente específico, escreva uma
explicação curta (2 a 4 frases) da causa provável do problema de sinal/conexão relatado,
para o atendimento usar ao explicar pro cliente ou registrar no chamado.

O histórico de causas de queda reportado pela própria OLT (quando presente nos dados) é
a fonte MAIS confiável que existe, mais até que o sinal do instante atual — o sinal agora
pode estar normal mesmo que o cliente tenha tido quedas recentes, e é exatamente isso que
normalmente motiva uma reclamação. Priorize esse histórico na sua resposta.

Regras:
- Não invente informação que não está nos dados.
- Se o sinal está normal agora mas o histórico da OLT mostra quedas recentes, reporte
  essas quedas e a causa que a OLT registrou (não diga apenas "está tudo normal").
- Se não houver histórico de causas da OLT disponível e o sinal/histórico de 30 dias
  também estiverem normais, aí sim diga que está tudo normal, sem forçar um problema.
- Se os dados não permitirem determinar a causa com confiança, diga isso explicitamente
  ao invés de especular.
- Não use travessão em nenhuma frase.
- Seja direto e técnico, sem saudação nem introdução."""

def _fmt_dado(v):
    return "não disponível" if v in (None, "") else v

def _smartolt_onu_detalhe(sn: str):
    """Status completo de uma ONU direto na OLT (comando ao vivo, ~5s,
    "resource-intensive" segundo a doc do SmartOLT — só pode ser chamado sob
    demanda pra debug pontual, NUNCA em bulk/polling). Aqui está restrito ao
    clique em 'Analisar causa provável', que já tem cache de 10min e trava de
    concorrência, então nunca dispara repetido para a mesma ONU.
    Traz atenuação óptica e o histórico real de causas reportado pela OLT
    (Power Fail, Optical Interference etc) — muito mais preciso que inferir
    causa só pelo nosso histórico de sinal."""
    try:
        r = requests.get(f"{SMARTOLT_URL}/api/onu/get_onu_full_status_info/{sn}", headers=HEADERS, timeout=15)
        r.raise_for_status()
        return r.json().get("full_status_json")
    except Exception as e:
        app.logger.warning(f"[IA] Falha ao buscar status completo da ONU {sn} no SmartOLT: {e}")
        return None

def _montar_contexto_cliente(d: dict, detalhe: dict | None = None) -> str:
    linhas = [
        f"Status atual da ONU: {_fmt_dado(d.get('status_hoje'))}",
        f"Sinal RX hoje (dBm): {_fmt_dado(d.get('rx_hoje'))}",
        f"Nível classificado: {_fmt_dado(d.get('nivel_hoje'))}",
        f"Última mudança de status: {_fmt_dado(d.get('ultima_mudanca'))}",
    ]
    rec = d.get("recorrente")
    if rec:
        linhas.append(
            f"Histórico 30 dias: {rec.get('dias_degradado')} dia(s) com sinal degradado, "
            f"pior RX {rec.get('pior_rx')} dBm, média {rec.get('media_rx')} dBm "
            f"(entre {rec.get('primeira_data')} e {rec.get('ultima_data')})"
        )
    else:
        linhas.append("Histórico 30 dias: nenhum dia com sinal degradado registrado.")
    piora = d.get("piora")
    if piora:
        linhas.append(
            f"Maior queda registrada: {piora.get('rx_anterior')} dBm em {piora.get('data_anterior')} "
            f"para {piora.get('rx_na_queda')} dBm em {piora.get('data_queda')}"
        )
    porta = d.get("porta_saude") or {}
    linhas.append(
        f"Porta compartilhada ({d.get('pon', '?')}): {porta.get('outros_afetados', 0)} de "
        f"{max(porta.get('total_onus', 1) - 1, 0)} outros clientes da mesma porta também afetados agora"
    )
    olt = d.get("olt_saude") or {}
    linhas.append(f"Saúde da OLT ({d.get('olt_name', '?')}): {olt.get('pct_offline', 0)}% offline, nível {olt.get('nivel', '?')}")

    if detalhe:
        onu_det = detalhe.get("ONU details") or {}
        if onu_det.get("ONU Distance"):
            linhas.append(f"Distância óptica até a OLT: {onu_det['ONU Distance']}")

        optico = detalhe.get("Optical status") or {}
        if optico.get("1310nm Attenuation") or optico.get("1490nm Attenuation"):
            linhas.append(
                f"Atenuação óptica medida pela OLT agora: 1310nm (upstream) "
                f"{optico.get('1310nm Attenuation', 'não disponível')}, 1490nm (downstream) "
                f"{optico.get('1490nm Attenuation', 'não disponível')}"
            )

        historico_olt = detalhe.get("History") or {}
        if historico_olt:
            try:
                chaves_ordenadas = sorted(historico_olt.keys(), key=lambda k: int(k))
            except ValueError:
                chaves_ordenadas = list(historico_olt.keys())
            eventos = []
            for chave in chaves_ordenadas[-5:]:
                ev = historico_olt[chave]
                causa = ev.get("Cause", "?")
                if ev.get("Offline at"):
                    eventos.append(f"{causa} (autenticou {ev.get('Auth at', '?')}, caiu {ev.get('Offline at', '?')})")
                else:
                    eventos.append(f"{causa} (desde {ev.get('Auth at', '?')})")
            linhas.append(
                "Histórico de causas de queda reportado pela própria OLT, eventos mais recentes: "
                + "; ".join(eventos)
            )

    return "\n".join(linhas)

@app.route("/api/consulta_cliente/causa_provavel", methods=["POST"])
def api_consulta_causa_provavel():
    sn = None
    try:
        dados = request.get_json(force=True) or {}
        sn = (dados.get("sn") or "").strip().upper()
        if not sn:
            return jsonify({"status": "erro", "mensagem": "sn é obrigatório."}), 400

        agora = time.time()
        cache = _CONSULTA_CAUSA_CACHE.get(sn)
        if cache and (agora - cache["ts"]) < cache["ttl"]:
            return jsonify({"status": "ok", "causa": cache["causa"], "cache": True,
                             "detalhe_smartolt": cache["detalhe_smartolt"]})

        if sn in _CONSULTA_CAUSA_EM_ANDAMENTO:
            return jsonify({"status": "erro",
                             "mensagem": "Já existe uma análise em andamento para esse cliente. Aguarde."}), 429
        _CONSULTA_CAUSA_EM_ANDAMENTO.add(sn)

        cliente = _gemini()
        if not cliente:
            return jsonify({"status": "erro", "mensagem": "IA não configurada (GEMINI_API_KEY ausente)."}), 503

        detalhe = _smartolt_onu_detalhe(sn)
        contexto = _montar_contexto_cliente(dados, detalhe)
        resposta = cliente.models.generate_content(
            model=GEMINI_MODEL,
            contents=contexto,
            config=types.GenerateContentConfig(
                system_instruction=_CAUSA_CLIENTE_PROMPT,
                temperature=0.2,
                max_output_tokens=250,
                thinking_config=types.ThinkingConfig(thinking_budget=0),
            ),
        )
        texto = (resposta.text or "").strip()
        if not texto:
            return jsonify({"status": "erro", "mensagem": "IA não retornou resposta."}), 502

        # Sem o detalhe ao vivo da OLT (endpoint "resource-intensive", pode falhar
        # ou ser limitado pelo SmartOLT), a análise fica incompleta — guarda por
        # menos tempo, pra não travar uma resposta pior por 10min à toa.
        detalhe_ok = detalhe is not None
        ttl = _CONSULTA_CAUSA_TTL if detalhe_ok else _CONSULTA_CAUSA_TTL_CURTO
        _CONSULTA_CAUSA_CACHE[sn] = {"causa": texto, "ts": agora, "ttl": ttl, "detalhe_smartolt": detalhe_ok}
        return jsonify({"status": "ok", "causa": texto, "cache": False, "detalhe_smartolt": detalhe_ok})
    except Exception as e:
        app.logger.error(f"[IA] Falha ao gerar causa provável (cliente): {e}")
        return jsonify({"status": "erro", "mensagem": "Falha ao consultar IA. Tente novamente."}), 500
    finally:
        if sn:
            _CONSULTA_CAUSA_EM_ANDAMENTO.discard(sn)

_GRAVIDADE_ORDEM = {"ok": 0, "atencao": 1, "critico": 2}

def _sobe_gravidade(atual, novo):
    return novo if _GRAVIDADE_ORDEM[novo] > _GRAVIDADE_ORDEM[atual] else atual

STATUS_CAUSA = {
    "Offline":    "o cliente está desconectado agora (equipamento offline)",
    "Power fail": "o cliente está desconectado agora, sem energia elétrica no equipamento",
    "LOS":        "o cliente está desconectado agora, com perda total de sinal óptico (possível rompimento de fibra)",
}

def _montar_veredito(status_hoje, nivel_hoje, recorrente, porta_saude, olt_saude):
    """Monta o texto de diagnóstico + nível de gravidade (ok/atencao/critico)."""
    motivos = []
    gravidade = "ok"

    if status_hoje and status_hoje != "Online":
        causa = STATUS_CAUSA.get(status_hoje, f"o cliente está desconectado agora (status '{status_hoje}')")
        motivos.append(f"{causa}.")
        gravidade = _sobe_gravidade(gravidade, "critico")
    elif nivel_hoje not in (NIVEL_NORMAL, NIVEL_SEM):
        motivos.append(f"o sinal está degradado agora (nível {nivel_hoje}).")
        nova = "critico" if nivel_hoje in (NIVEL_CRITICO, NIVEL_FORA) else "atencao"
        gravidade = _sobe_gravidade(gravidade, nova)

    if recorrente and recorrente["dias_degradado"] >= 2:
        motivos.append(
            f"detectamos degradação recorrente: {recorrente['dias_degradado']} dia(s) nos últimos 30 dias "
            f"(pior sinal {recorrente['pior_rx']} dBm, entre {recorrente['primeira_data']} e {recorrente['ultima_data']})."
        )
        nova = "critico" if recorrente["dias_degradado"] >= 5 else "atencao"
        gravidade = _sobe_gravidade(gravidade, nova)

    if porta_saude["outros_afetados"] > 0:
        motivos.append(
            f"outros {porta_saude['outros_afetados']} cliente(s) na mesma porta também estão afetados agora. "
            f"Provável problema de infraestrutura compartilhada (fibra/splitter), não do equipamento individual."
        )
        gravidade = _sobe_gravidade(gravidade, "critico")

    if olt_saude["nivel"] != "ok":
        motivos.append(
            f"a OLT inteira está em nível {olt_saude['nivel']} ({olt_saude['pct_offline']}% dos clientes ativos offline). "
            f"Pode haver um contexto mais amplo envolvido."
        )
        gravidade = _sobe_gravidade(gravidade, olt_saude["nivel"])

    if not motivos:
        texto = ("Sinal limpo em todas as camadas verificadas (ao vivo, histórico de 30 dias, porta compartilhada "
                 "e saúde da OLT). É pouco provável que a causa seja óptica. Sugerimos verificar Wi-Fi, "
                 "equipamento do cliente ou a camada de rede/banda.")
    else:
        prefixo = "Recomenda-se despachar campo: " if gravidade == "critico" else "Vale acompanhar: "
        texto = prefixo + " ".join(m[0].upper() + m[1:] for m in motivos)

    return texto, gravidade


# ── Painel consolidado (visão única de plantão) ────────────────
@app.route("/painel")
def painel_page(): return render_template("painel.html")

@app.route("/api/painel")
def api_painel():
    """Junta em uma tela só o que hoje exige abrir 3 telas separadas:
    outages de PON ativas agora, OLTs em atenção/crítico e chamados represados."""
    try:
        outages_data = _buscar_outages()
        outages_ativos = [o for o in outages_data.get("outages", [])]

        olts, olts_resumo, _atualizado = _calcular_saude_olts()
        olts_alerta = [o for o in olts if o["nivel"] != "ok"]

        sla = _chamados_sla(top_n=10)

        resumo = {
            "outages_ativos":      len(outages_ativos),
            "olts_atencao":        olts_resumo["atencao"],
            "olts_critico":        olts_resumo["critico"],
            "chamados_atencao":    sla["represados_atencao"],
            "chamados_critico":    sla["represados_critico"],
        }
        # Nível geral: crítico se qualquer contador crítico > 0, atenção se qualquer contador > 0
        if resumo["olts_critico"] or outages_ativos or resumo["chamados_critico"]:
            nivel_geral = "critico"
        elif resumo["olts_atencao"] or resumo["chamados_atencao"]:
            nivel_geral = "atencao"
        else:
            nivel_geral = "ok"

        return jsonify({
            "status": "ok",
            "nivel_geral": nivel_geral,
            "resumo": resumo,
            "outages": outages_ativos,
            "outages_resumo": outages_data.get("resumo", {}),
            "olts_alerta": olts_alerta,
            "sla": sla,
            "timestamp": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        })
    except Exception as e:
        return jsonify({"status": "erro", "mensagem": str(e)}), 500


# ── Refresh periódico do cache de conectividade ───────────────
# Mantém os indicadores (Total/Online/Não Online) atualizados sem depender
# do botão. Custo: 1 chamada get_all_onus_details por ciclo (traz todas as
# ONUs de uma vez). A cada 60s = 60 chamadas/hora, ainda com folga ampla no
# rate limit do SmartOLT (1.000/hora) — reduzido de 2h para refletir o
# status quase em tempo real ao investigar um caso (CONSULTA/SAÚDE).
CACHE_REFRESH_INTERVAL = int(os.getenv("OTDR_CACHE_REFRESH", "60"))  # 1 min

def _refresh_periodico():
    time.sleep(30)  # deixa o app subir antes do primeiro refresh
    while True:
        try:
            _, atualizado, _rl = get_onus(force=True)
            if atualizado:
                app.logger.info("Cache de conectividade renovado automaticamente.")
        except Exception as e:
            app.logger.warning(f"Falha no refresh periódico do cache: {e}")
        time.sleep(CACHE_REFRESH_INTERVAL)

threading.Thread(target=_refresh_periodico, daemon=True).start()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5008, debug=False)
