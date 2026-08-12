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
import psycopg2
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

# "latest_status_change" da API get_outage_pons já vem em horário local de
# Brasília (achávamos que era UTC, mas em 08/07/2026 confirmamos ao vivo que
# não é: o SmartOLT mostrava "X min atrás" batendo com o horário local, e o
# alerta batia com o horário real do evento só depois de subtrairmos 3h a
# mais por engano). Não converter mais, só normaliza o formato.

def _utc_para_brasilia(ts_str):
    if not ts_str:
        return ts_str
    try:
        dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return ts_str

SMTP_HOST     = os.getenv("SMTP_HOST",    "smtplw.com.br")
SMTP_PORT     = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER     = os.getenv("SMTP_USER",    "")
SMTP_PASS     = os.getenv("SMTP_PASS",    "")
SMTP_FROM     = os.getenv("SMTP_FROM",    "noreply@exemplo.com.br")
ALERT_TO      = os.getenv("OTDR_ALERT_EMAIL", "")

POLL_INTERVAL = int(os.getenv("OTDR_POLL_INTERVAL", "420"))   # 7 minutos
# Mínimo com efeito real: como a checagem só roda a cada POLL_INTERVAL, um
# cooldown menor que isso não muda nada (a próxima chance de alertar já é o
# próximo ciclo de qualquer forma). Por isso o piso é o próprio POLL_INTERVAL.
COOLDOWN_SEC  = int(os.getenv("OTDR_COOLDOWN_SEC",  "420"))   # 7 min por porta (mínimo útil)

# Limiar mínimo de impacto para alertar uma queda de porta: alerta se
# (quantidade absoluta de clientes afetados >= ALERTA_MIN_QTD) OU
# (percentual da porta afetado >= ALERTA_MIN_PCT). O "ou" cobre os dois casos
# que cada critério sozinho deixa passar: porta grande com impacto parcial mas
# relevante (ex: 15 de 60, 25%) e porta pequena praticamente inteira caída
# (ex: 4 de 6, 67%). Só fica de fora o que é baixo nos dois ao mesmo tempo.
ALERTA_MIN_QTD = int(os.getenv("OTDR_ALERTA_MIN_QTD", "10"))
ALERTA_MIN_PCT = float(os.getenv("OTDR_ALERTA_MIN_PCT", "50"))

# Queda de energia (power fail) tem limiar próprio, bem mais alto: é um
# problema fora do controle da empresa (concessionária), não acionável em
# pequena escala. Só é relevante quando em massa, quando indica necessidade
# de reautenticação coordenada das ONUs após a energia voltar.
ALERTA_MIN_QTD_POWER = int(os.getenv("OTDR_ALERTA_MIN_QTD_POWER", "100"))

# ── Saúde por OLT (degradação distribuída) ────────────────────
SAUDE_ATENCAO  = float(os.getenv("OTDR_SAUDE_ATENCAO", "10"))            # % offline
SAUDE_CRITICO  = float(os.getenv("OTDR_SAUDE_CRITICO", "18"))            # % offline
SAUDE_COOLDOWN = int(os.getenv("OTDR_SAUDE_COOLDOWN", "43200"))          # 12h por OLT
SAUDE_TO       = os.getenv("OTDR_SAUDE_EMAIL", ALERT_TO)                 # equipes técnicas (fallback: Fernando)
ESCALON_SEC    = int(os.getenv("OTDR_ESCALONAMENTO_HORAS", "24")) * 3600 # persistência p/ escalonar
ESCALON_EMAIL  = os.getenv("OTDR_ESCALONAMENTO_EMAIL", "")               # liderança (vazio = desligado)

# ── WhatsApp (Meta Cloud API — canal redundante ao e-mail) ─────
# Vazio = desligado (função não faz nada, e-mail continua sendo o canal principal).
WHATSAPP_TOKEN    = os.getenv("WHATSAPP_TOKEN", "")
WHATSAPP_PHONE_ID = os.getenv("WHATSAPP_PHONE_ID", "")
WHATSAPP_TEMPLATE = os.getenv("WHATSAPP_TEMPLATE", "otdr_alerta")
WHATSAPP_LANG     = os.getenv("WHATSAPP_LANG", "pt_BR")
WHATSAPP_TO       = os.getenv("WHATSAPP_TO", "")          # números com DDI, separados por vírgula (NOC)
WHATSAPP_ESCALON_TO = os.getenv("WHATSAPP_ESCALON_TO", "") # liderança (vazio = desligado, mesma lógica do ESCALON_EMAIL)

# ── SYNKR (Aprimorar) — avisos de parada para o call center ────
# Vazio = desligado (função não faz nada, e-mail/WhatsApp continuam sendo os
# canais internos; isso aqui é só o aviso para o call center terceirizado).
SYNKR_URL       = os.getenv("SYNKR_URL", "https://synkr.aprimorar.net.br/api").rstrip("/")
SYNKR_LOGIN     = os.getenv("SYNKR_LOGIN", "")
SYNKR_PASSWORD  = os.getenv("SYNKR_PASSWORD", "")
SYNKR_BUSINESS  = os.getenv("SYNKR_BUSINESS_NAME", "CANAA")
SYNKR_DEADLINE_HORAS = int(os.getenv("SYNKR_DEADLINE_HORAS", "4"))
SYNKR_AVISOS_FILE = BASE_DIR / "synkr_avisos.json"

CACHE_FILE        = BASE_DIR / "cache_onus.json"
SAUDE_ESTADO_FILE = BASE_DIR / "saude_estado.json"

# ── Histórico de alertas (início, fim, causa) ──────────────────
# Mesmo Postgres que já guarda o histórico de sinal (otdr.historico_smartolt).
# Canal independente dos demais: falha aqui não impede o alerta de sair.
PG_CONFIG = {
    "host":     os.getenv("PG_HOST"),
    "dbname":   os.getenv("PG_DATABASE"),
    "user":     os.getenv("PG_USER"),
    "password": os.getenv("PG_PASSWORD"),
    "port":     int(os.getenv("PG_PORT", 5432)),
}

def _pg_conectar():
    return psycopg2.connect(**PG_CONFIG)

def _garantir_tabela_historico() -> None:
    try:
        conn = _pg_conectar()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS otdr.alertas_historico (
                id SERIAL PRIMARY KEY,
                olt_id TEXT NOT NULL,
                olt_nome TEXT NOT NULL,
                board TEXT,
                porta TEXT,
                categoria TEXT,
                causa TEXT,
                onus_afetados INTEGER,
                onus_total INTEGER,
                inicio TIMESTAMP NOT NULL,
                fim TIMESTAMP,
                duracao_segundos INTEGER,
                synkr_notice_id INTEGER,
                criado_em TIMESTAMP DEFAULT now()
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_alertas_historico_inicio ON otdr.alertas_historico (inicio DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_alertas_historico_aberto ON otdr.alertas_historico (olt_id, board, porta) WHERE fim IS NULL")
        conn.commit()
        cur.close(); conn.close()
        log.info("[HISTORICO] Tabela otdr.alertas_historico pronta.")
    except Exception as e:
        log.error(f"[HISTORICO] Falha ao preparar tabela de histórico: {e}")

def _registrar_alerta_inicio(olt_id: str, olt_nome: str, porta: dict, inicio: datetime, notice_id: int | None) -> None:
    try:
        conn = _pg_conectar()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO otdr.alertas_historico
                (olt_id, olt_nome, board, porta, categoria, causa, onus_afetados, onus_total, inicio, synkr_notice_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            olt_id, olt_nome, str(porta.get("board", "")), str(porta.get("port", "")),
            porta.get("categoria", ""), CATEGORIA_LABEL.get(porta.get("categoria", ""), "Desconhecida"),
            _qtd_afetados(porta), porta.get("total_onus"), inicio, notice_id,
        ))
        conn.commit()
        cur.close(); conn.close()
    except Exception as e:
        log.error(f"[HISTORICO] Falha ao registrar início ({olt_nome} {porta.get('board')}/{porta.get('port')}): {e}")

def _registrar_alerta_fim(olt_id: str, board: str, porta_num: str, fim: datetime) -> None:
    """`fim` vem de datetime.now() do próprio processo Python (mesma fonte que
    `inicio` em _registrar_alerta_inicio), nunca de now() do SQL. O now() do
    Postgres depende do timezone configurado na sessão da conexão, que pode
    divergir do relógio local do processo (foi exatamente isso que gerava
    duração negativa: fim ficando "antes" do início por causa desse
    descompasso, não por um erro no dado em si)."""
    try:
        conn = _pg_conectar()
        cur = conn.cursor()
        cur.execute("""
            UPDATE otdr.alertas_historico
            SET fim = %s, duracao_segundos = EXTRACT(EPOCH FROM (%s::timestamp - inicio))::int
            WHERE id = (
                SELECT id FROM otdr.alertas_historico
                WHERE olt_id = %s AND board = %s AND porta = %s AND fim IS NULL
                ORDER BY inicio DESC LIMIT 1
            )
        """, (fim, fim, olt_id, board, porta_num))
        conn.commit()
        cur.close(); conn.close()
    except Exception as e:
        log.error(f"[HISTORICO] Falha ao registrar fim ({olt_id} {board}/{porta_num}): {e}")

# ── Estado em memória ─────────────────────────────────────────
_estado_anterior: dict[str, dict] = {}     # olt_id → {chave de porta: última porta em outage}
_ultimo_alerta: dict[str, datetime] = {}  # "olt_id:porta" → timestamp do último alerta
_aquecendo = True  # Primeira rodada: captura baseline sem enviar alertas
_processo_iniciado_em = datetime.now()  # usado pra não perder outage que começou bem na hora do restart

# saúde: olt_nome → {nivel, critico_desde, ultimo_alerta, escalonado}
_saude_estado: dict = {}

# "olt_id:porta" ou "saude:olt_nome" → stop_notice_id aberto no SYNKR
_synkr_avisos: dict[str, int] = {}


# ── SmartOLT API ──────────────────────────────────────────────
# Categoria = a seção do get_outage_pons onde a porta apareceu (é isso que
# distingue LOS parcial de LOS total, power fail e offline — o próprio
# SmartOLT já faz essa classificação, só precisamos ler certo).
CATEGORIA_LABEL = {
    "partial_los": "LOS parcial (parte dos clientes da porta)",
    "los":         "LOS total (porta inteira sem sinal, provável fibra cortada)",
    "power":       "Queda de energia (Power Fail)",
    "offline":     "Offline não identificado",
}

def get_outage_pons() -> list[dict]:
    """Consulta única e global: todas as portas em outage de todas as OLTs,
    já categorizadas pelo próprio SmartOLT. Resposta real da API:
    {"response": {"sections": [{"key": "partial_los"|"los"|"power"|"offline",
    "groups": [{"pons": [ {board, port, olt_id, olt_name, los_count, ...} ]}]}]}}
    """
    resp = requests.get(f"{SMARTOLT_URL}/api/system/get_outage_pons/", headers=HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    sections = (data.get("response") or {}).get("sections", [])
    portas = []
    for sec in sections:
        categoria = sec.get("key", "")
        for grupo in sec.get("groups", []):
            for p in grupo.get("pons", []):
                p = dict(p)
                p["categoria"] = categoria
                portas.append(p)
    return portas


# ── Helpers ───────────────────────────────────────────────────
def chave_porta(porta: dict) -> str:
    """Chave única da porta DENTRO do OLT (o agrupamento por olt_id já separa
    OLTs diferentes, então board:port não precisa incluir o olt_id)."""
    board = porta.get("board", porta.get("slot", 0))
    pon   = porta.get("port", porta.get("pon_port", 0))
    return f"{board}:{pon}"

def _qtd_afetados(porta: dict) -> int:
    """Quantidade absoluta de ONUs afetadas. Em outage total (los/power/
    offline) é a porta inteira (total_onus); em parcial, o campo já vem certo."""
    afetados = porta.get("affected_onus")
    if afetados is not None:
        try:
            return int(afetados)
        except (TypeError, ValueError):
            pass
    try:
        return int(porta.get("total_onus", 0) or 0)
    except (TypeError, ValueError):
        return 0

def _deve_alertar(porta: dict) -> bool:
    """Critério combinado: alerta se a quantidade absoluta de clientes
    afetados OU o percentual da porta ultrapassar o limiar.
    O percentual só é um sinal real em outages PARCIAIS (partial_los), onde o
    SmartOLT calcula de fato quantos clientes caíram dentro da porta. Em
    outages TOTAIS (los/offline) o percentual é sempre 100% por definição
    (a porta inteira caiu) e não discrimina nada, então nesses casos só a
    quantidade absoluta decide.
    "power" (queda de energia) tem limiar próprio, bem mais alto: fora do
    controle da empresa, só relevante em massa (ver ALERTA_MIN_QTD_POWER)."""
    qtd = _qtd_afetados(porta)
    categoria = porta.get("categoria")

    if categoria == "power":
        return qtd >= ALERTA_MIN_QTD_POWER

    if qtd >= ALERTA_MIN_QTD:
        return True
    if categoria == "partial_los":
        pct = porta.get("affected_percent")
        if pct is not None:
            try:
                return float(pct) >= ALERTA_MIN_PCT
            except (TypeError, ValueError):
                pass
    return False

def _comecou_apos_processo(porta: dict) -> bool:
    """Só relevante durante o aquecimento (1º ciclo após reiniciar o serviço):
    uma porta nova só é candidata a alerta se o próprio SmartOLT reportar que
    ela começou DEPOIS do processo ter subido. Senão é estado pré-existente
    (a porta já estava em outage antes do restart), que o aquecimento
    absorve como baseline sem alertar — evita alarme em massa toda vez que o
    serviço reinicia, mas sem esconder uma queda real que aconteceu bem na
    hora do restart (já vivemos esse caso: CEILANDIA 4/7 em 08/07/2026).

    Achado real 2026-08-12: pra categoria partial_los, o latest_status_change
    do SmartOLT vem sistematicamente ~30-35min NO FUTURO (confirmado ao vivo
    em 3 portas diferentes), não é hora de início real. Com isso sempre
    "parece" ter começado depois do restart, disparando alarme falso em toda
    outage parcial pré-existente sempre que o serviço reinicia (aconteceu 2x
    hoje: AGUAS CLARAS-2 13/9 e 3/13 no 1º restart, AGUAS CLARAS-1 6/9 no
    2º). Sem um jeito confiável de saber se uma outage parcial é nova,
    assume que não é (absorve como baseline): perder um alerta genuíno bem
    na janela do restart é bem menos custoso que falso alarme a cada deploy."""
    if porta.get("categoria") == "partial_los":
        return False
    ts = porta.get("latest_status_change")
    if not ts:
        return False
    try:
        inicio = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return False
    return inicio >= _processo_iniciado_em


# ── SYNKR (Aprimorar) — avisos de parada do call center ────────
def _carregar_synkr_avisos() -> dict:
    try:
        if SYNKR_AVISOS_FILE.exists():
            with open(SYNKR_AVISOS_FILE) as f:
                return json.load(f)
    except Exception as e:
        log.warning(f"[SYNKR] Falha ao carregar avisos salvos: {e}")
    return {}

def _salvar_synkr_avisos() -> None:
    try:
        with open(SYNKR_AVISOS_FILE, "w") as f:
            json.dump(_synkr_avisos, f)
    except Exception as e:
        log.warning(f"[SYNKR] Falha ao salvar avisos: {e}")

def _synkr_autenticar() -> str | None:
    """Login + seleção do negócio CANAA. Refeito a cada chamada (a doc não
    informa tempo de expiração do token, então não arriscamos cachear)."""
    if not (SYNKR_LOGIN and SYNKR_PASSWORD):
        return None
    try:
        r = requests.post(f"{SYNKR_URL}/auth/sign_in",
                           json={"login": SYNKR_LOGIN, "password": SYNKR_PASSWORD},
                           timeout=15)
        r.raise_for_status()
        token = r.json().get("token")
        if not token:
            log.error("[SYNKR] Login não retornou token.")
            return None

        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        r = requests.get(f"{SYNKR_URL}/me/business", headers=headers, timeout=15)
        r.raise_for_status()
        negocios = r.json().get("data", [])
        alvo = next((n for n in negocios if n.get("name") == SYNKR_BUSINESS), None)
        if not alvo:
            nomes = [n.get("name") for n in negocios]
            log.error(f"[SYNKR] Negócio '{SYNKR_BUSINESS}' não encontrado entre {nomes}.")
            return None

        r = requests.post(f"{SYNKR_URL}/me/business", headers=headers,
                           json={"id": alvo["id"]}, timeout=15)
        r.raise_for_status()
        return token
    except requests.exceptions.RequestException as e:
        log.error(f"[SYNKR] Falha na autenticação: {e}")
        return None

def _synkr_criar_aviso(chave: str, description: str, impact: str,
                        text_for_client: str, start_dt: datetime) -> None:
    """Cria um aviso de parada no SYNKR (call center Aprimorar). Canal
    independente de e-mail/WhatsApp — falha aqui não afeta os demais."""
    if chave in _synkr_avisos:
        return  # já existe aviso aberto para essa chave
    token = _synkr_autenticar()
    if not token:
        return
    try:
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        body = {
            "start_timestamp": start_dt.strftime("%Y-%m-%dT%H:%M:%S"),
            "description": description,
            "deadline": (start_dt + timedelta(hours=SYNKR_DEADLINE_HORAS)).strftime("%Y-%m-%dT%H:%M:%S"),
            "impact": impact,
            "text_for_client": text_for_client,
        }
        r = requests.post(f"{SYNKR_URL}/callcenter/notice", headers=headers, json=body, timeout=15)
        r.raise_for_status()
        notice_id = r.json().get("stop_notice_id")
        if notice_id is not None:
            _synkr_avisos[chave] = notice_id
            _salvar_synkr_avisos()
            log.info(f"[SYNKR] Aviso de parada criado (#{notice_id}) para {chave}")
    except requests.exceptions.RequestException as e:
        log.error(f"[SYNKR] Falha ao criar aviso para {chave}: {e}")

def _synkr_fechar_aviso(chave: str, report: str) -> None:
    """Encerra o aviso de parada correspondente à chave, se existir um aberto."""
    notice_id = _synkr_avisos.get(chave)
    if notice_id is None:
        return
    token = _synkr_autenticar()
    if not token:
        return
    try:
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        body = {
            "description": report,
            "deadline": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "impact": "Normalizado",
            "text_for_client": "O problema foi normalizado. Pedimos desculpas pelo transtorno.",
            "finished": True,
        }
        r = requests.post(f"{SYNKR_URL}/callcenter/notice/{notice_id}/report",
                           headers=headers, json=body, timeout=15)
        r.raise_for_status()
        log.info(f"[SYNKR] Aviso #{notice_id} encerrado ({chave})")
    except requests.exceptions.RequestException as e:
        log.error(f"[SYNKR] Falha ao encerrar aviso #{notice_id} ({chave}): {e}")
    finally:
        _synkr_avisos.pop(chave, None)
        _salvar_synkr_avisos()


# ── Email ─────────────────────────────────────────────────────
def enviar_alerta(olt_nome: str, porta: dict, chave_synkr: str) -> None:
    board = porta.get("board", "?")
    pon   = porta.get("port", "?")
    onus  = porta.get("total_onus", "?")
    los   = porta.get("los_count", 0)
    pwrf  = porta.get("power_count", 0)
    categoria = porta.get("categoria", "")
    causa = CATEGORIA_LABEL.get(categoria, "Desconhecida")
    agora = datetime.now().strftime("%d/%m/%Y %H:%M")
    # latest_status_change não é confiável pra partial_los (ver
    # _comecou_apos_processo): mostra a hora real do SmartOLT só pras
    # categorias onde ela bate (los/offline/power); pra partial_los, um
    # texto honesto de "detectado agora" em vez de afirmar um horário
    # errado. Some string não-parseável como "%Y-%m-%d %H:%M:%S" também já
    # faz o start_dt mais abaixo cair no fallback datetime.now() (mais
    # correto que usar o horário futuro nesse caso).
    if categoria == "partial_los":
        ts = f"detectado nesta varredura, {agora}"
    else:
        ts = _utc_para_brasilia(porta.get("latest_status_change", "—"))

    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#f3f4f6;font-family:Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0">
<tr><td align="center" style="padding:24px 12px;">
<table width="560" cellpadding="0" cellspacing="0"
       style="background:#fff;border-radius:8px;border:1px solid #e5e7eb;overflow:hidden;">

  <tr><td style="background:#b91c1c;padding:20px 28px;">
    <p style="margin:0;color:#fff;font-size:17px;font-weight:bold;">⚠️ Queda detectada: {olt_nome}</p>
    <p style="margin:4px 0 0;color:#fca5a5;font-size:12px;">{agora} · OTDR Preventivo Canaã</p>
  </td></tr>

  <tr><td style="padding:24px 28px;">

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
      Varredura automática · intervalo 7 min · cooldown 7 min por porta.
    </p>

  </td></tr>
</table>
</td></tr>
</table>
</body>
</html>"""

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"⚠️ OTDR · Queda detectada: {olt_nome}"
    msg["From"]    = SMTP_FROM
    msg["To"]      = ALERT_TO
    msg.attach(MIMEText(html, "html"))

    destinatarios = [e.strip() for e in ALERT_TO.split(",")]
    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as smtp:
            smtp.starttls()
            smtp.login(SMTP_USER, SMTP_PASS)
            smtp.sendmail(SMTP_FROM, destinatarios, msg.as_string())
        log.info(f"Alerta enviado → {ALERT_TO} | {olt_nome} porta {board}/{pon} | {causa}")
    except Exception as e:
        log.error(f"Falha ao enviar e-mail de queda ({olt_nome}): {e}")

    # Canal independente do e-mail — roda mesmo que o SMTP acima falhe.
    try:
        _enviar_whatsapp([
            f"Queda detectada: {olt_nome}",
            f"Porta {board}/{pon} · causa: {causa} · {onus} ONUs na porta",
            f"Início da ocorrência: {ts}",
        ])
    except Exception as e:
        log.error(f"[WHATSAPP] Falha ao enviar alerta de queda ({olt_nome}): {e}")

    # Aviso de parada para o call center (Aprimorar/SYNKR) — também
    # independente dos canais acima.
    try:
        try:
            start_dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError):
            start_dt = datetime.now()
        _synkr_criar_aviso(
            chave_synkr,
            description=f"Queda detectada na OLT {olt_nome}, porta {board}/{pon}. Causa: {causa}. {onus} ONUs na porta.",
            impact=f"Aproximadamente {onus} cliente(s) na porta {board}/{pon} da OLT {olt_nome}.",
            text_for_client="Identificamos uma instabilidade na conexão da sua região. Nossa equipe técnica já "
                             "está atuando na correção. Pedimos desculpas pelo transtorno.",
            start_dt=start_dt,
        )
    except Exception as e:
        log.error(f"[SYNKR] Falha ao processar aviso de queda ({olt_nome} porta {board}/{pon}): {e}")

    # Histórico (início, fim e causa) — canal independente dos demais.
    try:
        olt_id_hist = chave_synkr.split(":")[0]
        notice_id = _synkr_avisos.get(chave_synkr)
        _registrar_alerta_inicio(olt_id_hist, olt_nome, porta, start_dt, notice_id)
    except Exception as e:
        log.error(f"[HISTORICO] Falha ao registrar início ({olt_nome} porta {board}/{pon}): {e}")

def enviar_normalizacao_porta(olt_nome: str, chave: str, porta: dict, desde: datetime) -> None:
    """Avisa que uma queda de porta (qualquer categoria: LOS total, LOS parcial,
    power fail ou offline) que tinha disparado alerta foi normalizada. Só é
    chamada para portas que de fato geraram alerta, não para toda porta que
    sai da lista de outages (evita repetir alerta pra evento que nunca foi
    relevante o suficiente pra notificar)."""
    board = porta.get("board", "?")
    pon   = porta.get("port", "?")
    onus  = porta.get("total_onus", "?")
    causa = CATEGORIA_LABEL.get(porta.get("categoria", ""), "Desconhecida")
    agora = datetime.now()
    duracao = _dur_texto((agora - desde).total_seconds())

    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#f3f4f6;font-family:Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0">
<tr><td align="center" style="padding:24px 12px;">
<table width="560" cellpadding="0" cellspacing="0"
       style="background:#fff;border-radius:8px;border:1px solid #e5e7eb;overflow:hidden;">

  <tr><td style="background:#15803d;padding:20px 28px;">
    <p style="margin:0;color:#fff;font-size:17px;font-weight:bold;">✅ Normalizado: {olt_nome}</p>
    <p style="margin:4px 0 0;color:#bbf7d0;font-size:12px;">{datetime.now().strftime('%d/%m/%Y %H:%M')} · OTDR Preventivo Canaã</p>
  </td></tr>

  <tr><td style="padding:24px 28px;">

    <table width="100%" cellpadding="0" cellspacing="0"
           style="border:1px solid #e5e7eb;border-radius:6px;overflow:hidden;font-size:13px;">
      <tr style="background:#f0fdf4;">
        <td colspan="2" style="padding:10px 14px;font-weight:bold;color:#15803d;">
          Porta {board}/{pon}
        </td>
      </tr>
      <tr>
        <td style="padding:9px 14px;color:#6b7280;border-top:1px solid #f3f4f6;width:48%;">Causa original</td>
        <td style="padding:9px 14px;font-weight:bold;color:#111827;border-top:1px solid #f3f4f6;">{causa}</td>
      </tr>
      <tr style="background:#f9fafb;">
        <td style="padding:9px 14px;color:#6b7280;">ONUs na porta</td>
        <td style="padding:9px 14px;font-weight:bold;">{onus}</td>
      </tr>
      <tr>
        <td style="padding:9px 14px;color:#6b7280;border-top:1px solid #f3f4f6;">Duração da ocorrência</td>
        <td style="padding:9px 14px;font-weight:bold;color:#15803d;border-top:1px solid #f3f4f6;">{duracao}</td>
      </tr>
    </table>

    <p style="margin:20px 0 0;font-size:11px;color:#9ca3af;">
      Varredura automática · intervalo 7 min.
    </p>

  </td></tr>
</table>
</td></tr>
</table>
</body>
</html>"""

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"✅ OTDR · Normalizado: {olt_nome}"
    msg["From"]    = SMTP_FROM
    msg["To"]      = ALERT_TO
    msg.attach(MIMEText(html, "html"))

    destinatarios = [e.strip() for e in ALERT_TO.split(",")]
    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as smtp:
            smtp.starttls()
            smtp.login(SMTP_USER, SMTP_PASS)
            smtp.sendmail(SMTP_FROM, destinatarios, msg.as_string())
        log.info(f"Normalização enviada → {ALERT_TO} | {olt_nome} porta {board}/{pon}")
    except Exception as e:
        log.error(f"Falha ao enviar e-mail de normalização ({olt_nome}): {e}")

    # Canal independente do e-mail, roda mesmo que o SMTP acima falhe.
    try:
        _enviar_whatsapp([
            f"Normalizado: {olt_nome}",
            f"Porta {board}/{pon} · causa original: {causa} · {onus} ONUs na porta",
            f"Duração da ocorrência: {duracao}",
        ])
    except Exception as e:
        log.error(f"[WHATSAPP] Falha ao enviar normalização ({olt_nome}): {e}")

    try:
        _registrar_alerta_fim(str(porta.get("olt_id", "")), str(board), str(pon), agora)
    except Exception as e:
        log.error(f"[HISTORICO] Falha ao registrar fim ({olt_nome} porta {board}/{pon}): {e}")


# ── Saúde por OLT ─────────────────────────────────────────────
def _carregar_saude_estado() -> dict:
    try:
        if SAUDE_ESTADO_FILE.exists():
            with open(SAUDE_ESTADO_FILE) as f:
                return json.load(f)
    except Exception as e:
        log.warning(f"Falha ao carregar estado de saúde: {e}")
    return {}

def _salvar_saude_estado() -> None:
    try:
        with open(SAUDE_ESTADO_FILE, "w") as f:
            json.dump(_saude_estado, f)
    except Exception as e:
        log.warning(f"Falha ao salvar estado de saúde: {e}")

def calcular_saude() -> list[dict]:
    """Consome /api/saude_olt do dashboard (fonte única). A métrica já considera
    apenas CLIENTES ATIVOS — ONUs canceladas/órfãs não contam para o alerta."""
    try:
        r = requests.get("http://127.0.0.1:5008/api/saude_olt", timeout=15)
        r.raise_for_status()
        data = r.json()
        if data.get("status") != "ok":
            return []
        res = []
        for o in data.get("olts", []):
            res.append({
                "olt":          o.get("olt", "—"),
                "total":        o.get("total", 0),
                "offline":      o.get("offline", 0),
                "power_fail":   o.get("power_fail", 0),
                "los":          o.get("los", 0),
                "offline_puro": o.get("offline_puro", 0),
                "pct":          o.get("pct_offline", 0),
                "nivel":        o.get("nivel", "ok"),
            })
        return res
    except Exception as e:
        log.warning(f"[SAÚDE] Falha ao consultar /api/saude_olt: {e}")
        return []

def _dur_texto(segundos: float) -> str:
    h = int(segundos // 3600)
    if h < 1:
        return f"{int(segundos // 60)} min"
    if h < 24:
        return f"{h}h"
    return f"{h // 24}d {h % 24}h"

def _html_saude(o: dict, titulo: str, cor: str, intro: str, extra: str = "") -> str:
    agora = datetime.now().strftime("%d/%m/%Y %H:%M")
    return f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#f3f4f6;font-family:Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0"><tr><td align="center" style="padding:24px 12px;">
<table width="560" cellpadding="0" cellspacing="0" style="background:#fff;border-radius:8px;border:1px solid #e5e7eb;overflow:hidden;">
  <tr><td style="background:{cor};padding:20px 28px;">
    <p style="margin:0;color:#fff;font-size:17px;font-weight:bold;">{titulo}</p>
    <p style="margin:4px 0 0;color:rgba(255,255,255,.75);font-size:12px;">{agora} · OTDR Preventivo Canaã</p>
  </td></tr>
  <tr><td style="padding:24px 28px;">
    <p style="margin:0 0 18px;font-size:14px;color:#374151;line-height:1.5;">{intro}</p>
    <table width="100%" cellpadding="0" cellspacing="0" style="border:1px solid #e5e7eb;border-radius:6px;overflow:hidden;font-size:13px;">
      <tr style="background:#f9fafb;"><td colspan="2" style="padding:10px 14px;font-weight:bold;color:#111827;">{o['olt']}</td></tr>
      <tr><td style="padding:9px 14px;color:#6b7280;border-top:1px solid #f3f4f6;width:52%;">Clientes offline</td>
          <td style="padding:9px 14px;font-weight:bold;color:{cor};border-top:1px solid #f3f4f6;">{o['pct']}% ({o['offline']} de {o['total']})</td></tr>
      <tr style="background:#f9fafb;"><td style="padding:9px 14px;color:#6b7280;">Sem energia (Power Fail)</td>
          <td style="padding:9px 14px;font-weight:bold;">{o['power_fail']}</td></tr>
      <tr><td style="padding:9px 14px;color:#6b7280;border-top:1px solid #f3f4f6;">Offline</td>
          <td style="padding:9px 14px;font-weight:bold;border-top:1px solid #f3f4f6;">{o['offline_puro']}</td></tr>
      <tr style="background:#f9fafb;"><td style="padding:9px 14px;color:#6b7280;">Sem sinal (LOS)</td>
          <td style="padding:9px 14px;font-weight:bold;">{o['los']}</td></tr>
    </table>
    {extra}
    <p style="margin:20px 0 0;font-size:11px;color:#9ca3af;">Saúde por OLT · limiar crítico ≥ {SAUDE_CRITICO:.0f}% · verificação automática.</p>
  </td></tr>
</table></td></tr></table></body></html>"""

def _enviar_email(assunto: str, html: str, destino: str) -> None:
    msg = MIMEMultipart("alternative")
    msg["Subject"] = assunto
    msg["From"]    = SMTP_FROM
    msg["To"]      = destino
    msg.attach(MIMEText(html, "html"))
    destinatarios = [e.strip() for e in destino.split(",") if e.strip()]
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as smtp:
        smtp.starttls()
        smtp.login(SMTP_USER, SMTP_PASS)
        smtp.sendmail(SMTP_FROM, destinatarios, msg.as_string())

def _enviar_whatsapp(parametros: list[str], destino: str = None) -> None:
    """Envia o mesmo alerta por WhatsApp (Meta Cloud API oficial), como canal
    redundante ao e-mail — nunca substituto. Requer template pré-aprovado no
    Meta Business Manager: mensagens iniciadas pela empresa (fora da janela de
    24h de conversa) só podem usar templates, não texto livre.
    Sem WHATSAPP_TOKEN/WHATSAPP_PHONE_ID configurados, não faz nada (silencioso)."""
    if not (WHATSAPP_TOKEN and WHATSAPP_PHONE_ID):
        return
    # destino=None → usa WHATSAPP_TO (padrão); destino="" (string vazia explícita)
    # → canal desligado para esta chamada (mesma lógica de ESCALON_EMAIL vazio).
    alvo = WHATSAPP_TO if destino is None else destino
    numeros = [n.strip() for n in alvo.split(",") if n.strip()]
    if not numeros:
        return
    url = f"https://graph.facebook.com/v20.0/{WHATSAPP_PHONE_ID}/messages"
    headers = {"Authorization": f"Bearer {WHATSAPP_TOKEN}", "Content-Type": "application/json"}
    for numero in numeros:
        body = {
            "messaging_product": "whatsapp",
            "to": numero,
            "type": "template",
            "template": {
                "name": WHATSAPP_TEMPLATE,
                "language": {"code": WHATSAPP_LANG},
                "components": [{
                    "type": "body",
                    "parameters": [{"type": "text", "text": str(p)} for p in parametros],
                }],
            },
        }
        try:
            r = requests.post(url, headers=headers, json=body, timeout=15)
            r.raise_for_status()
            log.info(f"[WHATSAPP] Alerta enviado → {numero}")
        except requests.exceptions.RequestException as e:
            detalhe = e.response.text if getattr(e, "response", None) is not None else str(e)
            log.error(f"[WHATSAPP] Falha ao enviar para {numero}: {detalhe}")

def enviar_alerta_saude(o: dict) -> None:
    html = _html_saude(
        o, f"🔴 OLT crítica: {o['olt']}", "#b91c1c",
        f"A OLT <strong>{o['olt']}</strong> ultrapassou o limiar crítico de clientes offline "
        f"(<strong>{o['pct']}%</strong>). Isso indica degradação distribuída. Recomenda-se "
        f"verificação de campo/NOC (possível queda de energia na região, backbone ou fibra).")
    try:
        _enviar_email(f"🔴 OTDR · OLT crítica: {o['olt']} ({o['pct']}% offline)", html, SAUDE_TO)
        log.info(f"[SAÚDE] Alerta crítico enviado → {SAUDE_TO} | {o['olt']} {o['pct']}%")
    except Exception as e:
        log.error(f"[SAÚDE] Falha ao enviar e-mail de alerta crítico ({o['olt']}): {e}")

    # Canal independente do e-mail — roda mesmo que o SMTP acima falhe.
    try:
        _enviar_whatsapp([
            f"OLT crítica: {o['olt']}",
            f"{o['pct']}% offline ({o['offline']} de {o['total']} clientes ativos)",
            "Recomenda-se verificação de campo/NOC.",
        ])
    except Exception as e:
        log.error(f"[WHATSAPP] Falha ao enviar alerta de saúde ({o['olt']}): {e}")

    # Aviso de parada para o call center (Aprimorar/SYNKR).
    try:
        _synkr_criar_aviso(
            f"saude:{o['olt']}",
            description=f"OLT {o['olt']} em estado crítico: {o['pct']}% dos clientes ativos offline.",
            impact=f"Aproximadamente {o['offline']} cliente(s) na região atendida pela OLT {o['olt']}.",
            text_for_client="Identificamos uma instabilidade que pode afetar sua conexão. Nossa equipe já está "
                             "trabalhando na normalização. Pedimos desculpas pelo transtorno.",
            start_dt=datetime.now(),
        )
    except Exception as e:
        log.error(f"[SYNKR] Falha ao processar aviso de saúde ({o['olt']}): {e}")

def enviar_normalizacao(o: dict) -> None:
    html = _html_saude(
        o, f"✅ OLT normalizada: {o['olt']}", "#15803d",
        f"A OLT <strong>{o['olt']}</strong> voltou a operar dentro do normal "
        f"(<strong>{o['pct']}%</strong> de clientes offline, abaixo do limiar crítico de {SAUDE_CRITICO:.0f}%).")
    try:
        _enviar_email(f"✅ OTDR · OLT normalizada: {o['olt']}", html, SAUDE_TO)
        log.info(f"[SAÚDE] Normalização enviada → {SAUDE_TO} | {o['olt']} {o['pct']}%")
    except Exception as e:
        log.error(f"[SAÚDE] Falha ao enviar e-mail de normalização ({o['olt']}): {e}")

    # Canal independente do e-mail — roda mesmo que o SMTP acima falhe.
    try:
        _enviar_whatsapp([
            f"OLT normalizada: {o['olt']}",
            f"{o['pct']}% offline, abaixo do limiar crítico de {SAUDE_CRITICO:.0f}%",
            "Sem ação necessária.",
        ])
    except Exception as e:
        log.error(f"[WHATSAPP] Falha ao enviar normalização ({o['olt']}): {e}")

    try:
        _synkr_fechar_aviso(f"saude:{o['olt']}",
                             report=f"OLT {o['olt']} normalizada ({o['pct']}% offline, abaixo do limiar crítico).")
    except Exception as e:
        log.error(f"[SYNKR] Falha ao encerrar aviso de saúde ({o['olt']}): {e}")

def enviar_escalonamento(o: dict, desde_ts: float) -> None:
    dur = _dur_texto(time.time() - desde_ts)
    extra = (f'<p style="margin:16px 0 0;padding:12px 14px;background:#fef2f2;border-left:3px solid #b91c1c;'
             f'border-radius:4px;font-size:13px;color:#7f1d1d;">Esta OLT permanece em estado crítico há '
             f'<strong>{dur}</strong> sem normalização. Escalonamento automático para a diretoria.</p>')
    html = _html_saude(
        o, f"⚠️ Escalonamento: {o['olt']} crítica há {dur}", "#7c2d12",
        f"A OLT <strong>{o['olt']}</strong> está em estado crítico "
        f"(<strong>{o['pct']}%</strong> offline) de forma persistente, sem retornar ao normal.", extra)
    try:
        _enviar_email(f"⚠️ OTDR · Escalonamento: {o['olt']} crítica há {dur}", html, ESCALON_EMAIL)
        log.info(f"[SAÚDE] Escalonamento enviado → {ESCALON_EMAIL} | {o['olt']} há {dur}")
    except Exception as e:
        log.error(f"[SAÚDE] Falha ao enviar e-mail de escalonamento ({o['olt']}): {e}")

    # Canal independente do e-mail — roda mesmo que o SMTP acima falhe.
    try:
        _enviar_whatsapp([
            f"Escalonamento: {o['olt']} crítica há {dur}",
            f"{o['pct']}% offline, sem normalização",
            "Ação da liderança recomendada.",
        ], destino=WHATSAPP_ESCALON_TO)
    except Exception as e:
        log.error(f"[WHATSAPP] Falha ao enviar escalonamento ({o['olt']}): {e}")

def verificar_saude() -> None:
    """Detecta OLTs que entram em estado crítico e dispara alertas (transição + cooldown)."""
    saude = calcular_saude()
    if not saude:
        return
    agora = time.time()

    for o in saude:
        olt = o["olt"]
        est = _saude_estado.get(olt, {"nivel": "ok", "critico_desde": None,
                                      "ultimo_alerta": 0, "escalonado": False})
        nivel_ant = est.get("nivel", "ok")

        if o["nivel"] == "critico":
            if nivel_ant != "critico":
                # transição para crítico
                est["critico_desde"] = agora
                est["escalonado"] = False
                if agora - est.get("ultimo_alerta", 0) >= SAUDE_COOLDOWN:
                    try:
                        enviar_alerta_saude(o)
                        est["ultimo_alerta"] = agora
                    except Exception as e:
                        log.error(f"[SAÚDE] Falha ao alertar {olt}: {e}")
                else:
                    log.info(f"[SAÚDE] {olt} voltou a crítico dentro do cooldown — sem novo e-mail.")
            # escalonamento por persistência
            if (ESCALON_EMAIL and not est.get("escalonado") and est.get("critico_desde")
                    and agora - est["critico_desde"] >= ESCALON_SEC):
                try:
                    enviar_escalonamento(o, est["critico_desde"])
                    est["escalonado"] = True
                except Exception as e:
                    log.error(f"[SAÚDE] Falha ao escalonar {olt}: {e}")
        else:
            if nivel_ant == "critico":
                try:
                    enviar_normalizacao(o)
                except Exception as e:
                    log.error(f"[SAÚDE] Falha ao notificar normalização {olt}: {e}")
                est["critico_desde"] = None
                est["escalonado"] = False

        est["nivel"] = o["nivel"]
        _saude_estado[olt] = est

    _salvar_saude_estado()


# ── Ciclo principal ───────────────────────────────────────────
def verificar() -> None:
    global _aquecendo

    try:
        portas_todas = get_outage_pons()
    except Exception as e:
        log.warning(f"Erro ao consultar get_outage_pons: {e}")
        return

    label = " [aquecimento]" if _aquecendo else ""
    log.info(f"Verificando {len(portas_todas)} porta(s) em outage (consulta global){label}...")

    # Agrupa por OLT (a resposta global já traz olt_id/olt_name em cada porta)
    por_olt: dict[str, list[dict]] = {}
    nome_por_olt: dict[str, str] = {}
    for p in portas_todas:
        oid = str(p.get("olt_id", "")).strip()
        if not oid:
            continue
        por_olt.setdefault(oid, []).append(p)
        nome_por_olt[oid] = p.get("olt_name", oid)

    alertas_enviados = 0

    for olt_id, portas in por_olt.items():
        olt_nome = nome_por_olt[olt_id]
        chaves_atuais = {chave_porta(p): p for p in portas}
        chaves_ant    = _estado_anterior.get(olt_id, {})
        novas_portas  = set(chaves_atuais.keys()) - set(chaves_ant.keys())
        recuperadas   = set(chaves_ant.keys()) - set(chaves_atuais.keys())

        # Porta que estava em outage e não aparece mais nesta OLT: recuperou.
        # Fecha o aviso de parada correspondente no SYNKR, se existir um aberto,
        # e avisa a normalização (WhatsApp/e-mail) só pra quem tinha gerado
        # alerta de verdade (evita normalização de evento que nunca notificou).
        for chave in recuperadas:
            try:
                _synkr_fechar_aviso(f"{olt_id}:{chave}",
                                    report=f"Porta {chave} da OLT {olt_nome} normalizada.")
            except Exception as e:
                log.error(f"[SYNKR] Falha ao encerrar aviso de porta ({olt_nome} {chave}): {e}")

            chave_cooldown = f"{olt_id}:{chave}"
            if chave_cooldown in _ultimo_alerta:
                try:
                    enviar_normalizacao_porta(olt_nome, chave, chaves_ant[chave], _ultimo_alerta[chave_cooldown])
                except Exception as e:
                    log.error(f"Falha ao enviar normalização de porta ({olt_nome} {chave}): {e}")

        if novas_portas:
            # No aquecimento, só avalia pra alerta as portas que comprovadamente
            # começaram DEPOIS do processo subir — o resto é baseline pré-existente.
            portas_candidatas = (
                {k for k in novas_portas if _comecou_apos_processo(chaves_atuais[k])}
                if _aquecendo else novas_portas
            )
            portas_qualificadas = {k for k in portas_candidatas if _deve_alertar(chaves_atuais[k])}

            if not portas_qualificadas:
                if _aquecendo:
                    log.info(f"{olt_nome}: {len(novas_portas)} porta(s) em outage no aquecimento "
                             f"({len(portas_candidatas)} começaram após o restart) — sem alerta.")
                else:
                    log.info(f"{olt_nome}: {len(novas_portas)} nova(s) porta(s) abaixo do limiar de impacto "
                             f"(>={ALERTA_MIN_QTD} clientes ou >={ALERTA_MIN_PCT}% da porta) — sem alerta.")
            else:
                # Cooldown por PORTA, não por OLT inteira — uma porta nova sempre
                # alerta na hora, mesmo se outra porta da mesma OLT alertou há
                # pouco. O cooldown só suprime a MESMA porta oscilando de novo.
                for chave in portas_qualificadas:
                    chave_cooldown = f"{olt_id}:{chave}"
                    ultimo = _ultimo_alerta.get(chave_cooldown)
                    em_cooldown = ultimo and (datetime.now() - ultimo).total_seconds() < COOLDOWN_SEC

                    if em_cooldown:
                        retoma = (ultimo + timedelta(seconds=COOLDOWN_SEC)).strftime("%H:%M")
                        log.info(f"{olt_nome} porta {chave}: nova outage — cooldown até {retoma}")
                    else:
                        try:
                            enviar_alerta(olt_nome, chaves_atuais[chave], chave_cooldown)
                            _ultimo_alerta[chave_cooldown] = datetime.now()
                            alertas_enviados += 1
                        except Exception as e:
                            log.error(f"Falha ao enviar alerta para {olt_nome} porta {chave}: {e}")

        _estado_anterior[olt_id] = dict(chaves_atuais)

    # OLTs que tinham outage e SAÍRAM da lista global (recuperaram): zera o
    # estado para que uma outage futura na mesma porta seja detectada de novo,
    # fecha os avisos de parada de todas as portas que ainda estavam abertas,
    # e avisa a normalização das que tinham gerado alerta de verdade.
    for olt_id in list(_estado_anterior.keys()):
        if olt_id not in por_olt:
            olt_nome_recuperada = None
            for chave, porta_ant in _estado_anterior[olt_id].items():
                olt_nome_recuperada = porta_ant.get("olt_name", olt_id)
                try:
                    _synkr_fechar_aviso(f"{olt_id}:{chave}",
                                        report="OLT normalizada, porta não está mais em outage.")
                except Exception as e:
                    log.error(f"[SYNKR] Falha ao encerrar aviso ({olt_id} {chave}): {e}")

                chave_cooldown = f"{olt_id}:{chave}"
                if chave_cooldown in _ultimo_alerta:
                    try:
                        enviar_normalizacao_porta(olt_nome_recuperada, chave, porta_ant, _ultimo_alerta[chave_cooldown])
                    except Exception as e:
                        log.error(f"Falha ao enviar normalização de porta ({olt_nome_recuperada} {chave}): {e}")
            _estado_anterior[olt_id] = {}

    if _aquecendo:
        total = sum(len(v) for v in _estado_anterior.values())
        log.info(f"Baseline capturado: {total} porta(s) em outage em {len(por_olt)} OLT(s) com outage ativa. Monitoramento ativo.")
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

    global _saude_estado, _synkr_avisos
    _saude_estado  = _carregar_saude_estado()
    _synkr_avisos  = _carregar_synkr_avisos()
    _garantir_tabela_historico()

    log.info(f"OTDR Alertas iniciado | URL: {SMARTOLT_URL}")
    log.info(f"Outage de porta → {ALERT_TO} (intervalo {POLL_INTERVAL}s, cooldown {COOLDOWN_SEC}s)")
    log.info(f"Saúde crítica (≥{SAUDE_CRITICO:.0f}%) → {SAUDE_TO} (cooldown {SAUDE_COOLDOWN//3600}h)" +
             (f" | escalona → {ESCALON_EMAIL} após {ESCALON_SEC//3600}h" if ESCALON_EMAIL else " | escalonamento desligado"))
    log.info(f"SYNKR (call center Aprimorar): {'ativo' if (SYNKR_LOGIN and SYNKR_PASSWORD) else 'desligado (sem credenciais)'}")

    while True:
        try:
            verificar()
        except KeyboardInterrupt:
            log.info("Encerrado pelo usuário.")
            break
        except Exception as e:
            log.error(f"Erro no ciclo de outage: {e}")

        try:
            verificar_saude()
        except Exception as e:
            log.error(f"Erro no ciclo de saúde: {e}")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
