"""
OTDR Preventivo CANAÃ: cliente do OLT Cloud
Substitui o SmartOLT, descontinuado em 2026-09 (nada no OTDR consulta mais o SmartOLT).

Entrega os dados no MESMO formato que o código do OTDR já consumia do SmartOLT:
nomes de campo (sn, board, port, onu, signal_1310...), vocabulário de status
("Power fail", "LOS", "Offline") e olt_id antigo. Assim o dashboard, o detector,
a tabela otdr.historico_smartolt e o Diagnóstico do Canaã Performance (que lê
essa tabela) continuam lendo igual. Toda tradução entre os dois sistemas mora
aqui, em um lugar só.

Correspondências confirmadas com dado real em 24/09/2026, cruzando 7.604 ONUs
das duas fontes no mesmo dia:
- signal_1310 (SmartOLT, RX na OLT)  = olt_rx (OLT Cloud), diferença média 0,15 dB
- signal_1490 (SmartOLT, RX na ONU)  = device_rx (OLT Cloud), diferença média 0,15 dB
- "Power fail" = "Sem Energia", "LOS" = "LOSS", "Offline" = "Inativo"
- nome da OLT e modelo da ONU são iguais nos dois sistemas
"""

import base64
import json
import logging
import math
import os
import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta

import requests

log = logging.getLogger(__name__)

# ── Tradução para o vocabulário do SmartOLT ───────────────────
STATUS_LEGADO = {
    "Online":      "Online",
    "Sem Energia": "Power fail",
    "LOSS":        "LOS",
    "Inativo":     "Offline",
}

# olt_id do SmartOLT por nome de OLT (os nomes são iguais nos dois sistemas).
# O id antigo está gravado em otdr.alertas_historico e nas chaves de
# synkr_avisos.json ("olt_id:board:porta"): com outra numeração, alerta aberto
# antes da troca nunca seria fechado. OLT nova, que não existia no SmartOLT,
# recebe "oc<id do OLT Cloud>".
OLT_ID_LEGADO = {
    "AGUAS CLARAS-1": "18",
    "AGUAS CLARAS-2": "19",
    "AGUAS CLARAS-3": "20",
    "TAGUATINGA-N1":  "21",
    "TAGUATINGA-N2":  "22",
    "CEILANDIA":      "23",
    "SUDOESTE":       "24",
    "SIA":            "25",
    "VICENTE PIRES":  "26",
    "ARNIQUEIRAS":    "27",
}

# O SmartOLT tinha uma zona só para a rede inteira.
ZONA_LEGADA = "CANAA"

# Leitura inválida no OLT Cloud (ONU sem luz). O SmartOLT devolvia vazio.
SINAL_INVALIDO = -90.0

FMT_DATA = "%Y-%m-%d %H:%M:%S"
TAMANHO_PAGINA = 1000


def _data(txt) -> datetime | None:
    """Aceita "2026-09-18T11:57:18.001648", "2026-09-18 11:57:18" e "18/09/2026 11:57:18".
    O OLT Cloud entrega horário local de Brasília (conferido no painel em 24/09/2026)."""
    if not txt:
        return None
    s = str(txt).strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S", FMT_DATA, "%d/%m/%Y %H:%M:%S"):
        try:
            return datetime.strptime(s[:19], fmt)
        except ValueError:
            continue
    return None


def _txt_data(txt) -> str | None:
    d = _data(txt)
    return d.strftime(FMT_DATA) if d else None


def _sinal(valor) -> float | None:
    try:
        v = float(valor)
    except (TypeError, ValueError):
        return None
    return v if v > SINAL_INVALIDO else None


def classe_sinal(rx_olt: float | None) -> str | None:
    """Mesma classe que o SmartOLT gravava em "signal", medida pelo RX na OLT.
    Faixas tiradas das 15.977 ONUs do último cache do SmartOLT (24/09/2026):
    Very good acima de -26 dBm, Warning de -26 a -30, Critical em -30 ou menos."""
    if rx_olt is None:
        return None
    if rx_olt > -26:
        return "Very good"
    if rx_olt > -30:
        return "Warning"
    return "Critical"


def _texto(v) -> str:
    return "" if v is None else str(v)


def para_formato_smartolt(eq: dict, manter_ultimo_sinal: bool = False) -> dict:
    """Um equipamento do OLT Cloud no formato de uma ONU do get_all_onus_details.

    ONU fora do ar sai sem sinal: o OLT Cloud mantém a última leitura de antes
    da queda, e no dashboard/detector ela não pode ser lida como sinal atual.
    `manter_ultimo_sinal=True` é só para o snapshot diário: lá a ONU fora do ar
    entra com a última leitura válida, como o SmartOLT fazia com parte delas
    (163 linhas no snapshot de 24/09/2026). É dessas linhas que o Diagnóstico
    do Canaã Performance tira "Power fail" x "LOS" para um cliente fora do ar.

    "name" vira "id<cliente IXC>" quando o OLT Cloud tem o vínculo (95% das ONUs):
    é o formato que identificar_cliente() já usava como segunda via de achar o
    cliente, e no SmartOLT só 38% das ONUs tinham esse nome.
    """
    status = STATUS_LEGADO.get(eq.get("status"), eq.get("status"))
    com_sinal = status == "Online" or manter_ultimo_sinal
    rx_olt = _sinal(eq.get("olt_rx")) if com_sinal else None
    rx_onu = _sinal(eq.get("device_rx")) if com_sinal else None
    olt = eq.get("olt") or ""
    cliente_ixc = eq.get("external_client_id")
    return {
        "sn":                    (eq.get("serial_number") or "").strip().upper(),
        "olt_id":                OLT_ID_LEGADO.get(olt, f"oc{eq.get('olt_id')}"),
        "olt_name":              olt,
        "board":                 _texto(eq.get("slot")),
        "port":                  _texto(eq.get("pon")),
        "onu":                   _texto(eq.get("onu_id")),
        "onu_type_name":         eq.get("model") or "",
        "zone_name":             ZONA_LEGADA,
        "name":                  f"id{cliente_ixc}" if cliente_ixc else (eq.get("device_alias") or ""),
        "address":               "",
        "status":                status,
        "signal":                classe_sinal(rx_olt),
        "signal_1310":           None if rx_olt is None else str(round(rx_olt, 3)),
        "signal_1490":           None if rx_onu is None else str(round(rx_onu, 3)),
        "last_status_change":    _txt_data(eq.get("last_status_update")),
        "administrative_status": "Enabled",
        "is_failed_resync_config": "0",
        # Campos só do OLT Cloud (o código legado ignora chaves que não conhece)
        "oltcloud_id":           eq.get("id"),
        "oltcloud_pon_id":       eq.get("pon_id"),
        "cliente_ixc":           cliente_ixc,
        "contrato_ixc":          eq.get("external_client_contract_id"),
        "last_signal_update":    _txt_data(eq.get("last_signal_update")),
        "uptime_since":          _txt_data(eq.get("uptime_since")),
        "macs":                  eq.get("macs") or [],
        "temperatura":           eq.get("temperature"),
    }


# ── Cliente HTTP ──────────────────────────────────────────────
class OltCloudErro(Exception):
    pass


def _exp_jwt(token: str) -> float:
    try:
        corpo = token.split(".")[1]
        corpo += "=" * (-len(corpo) % 4)
        return float(json.loads(base64.urlsafe_b64decode(corpo)).get("exp", 0))
    except Exception:
        return 0.0


class OltCloud:
    """Login por usuário e senha (POST /api/token) com renovação automática.

    O access vale 3 h e o refresh 24 h, e o refresh NÃO devolve um refresh
    novo (conferido em 24/09/2026): depois de 24 h o cliente loga de novo com
    a senha sozinho. Seguro para uso em várias threads (o dashboard usa).
    """

    def __init__(self, url: str | None = None, usuario: str | None = None, senha: str | None = None,
                 timeout: int = 60):
        self.url = (url if url is not None else os.getenv("OLTCLOUD_URL", "")).rstrip("/")
        self.usuario = usuario if usuario is not None else os.getenv("OLTCLOUD_USER", "")
        self.senha = senha if senha is not None else os.getenv("OLTCLOUD_PASSWORD", "")
        self.timeout = timeout
        self._lock = threading.Lock()
        self._access = None
        self._access_exp = 0.0
        self._refresh = None
        self._refresh_exp = 0.0
        self._sessao = requests.Session()

    def configurado(self) -> bool:
        return bool(self.url and self.usuario and self.senha)

    def _post_token(self, caminho: str, corpo: dict) -> dict:
        r = self._sessao.post(f"{self.url}{caminho}", json=corpo, timeout=self.timeout)
        if r.status_code != 200:
            raise OltCloudErro(f"{caminho} respondeu {r.status_code}")
        return r.json()

    def _logar(self) -> None:
        d = self._post_token("/api/token", {"username": self.usuario, "password": self.senha})
        self._access, self._refresh = d["access"], d.get("refresh")
        self._access_exp = _exp_jwt(self._access)
        self._refresh_exp = _exp_jwt(self._refresh) if self._refresh else 0.0

    def _token(self, forcar_novo: bool = False) -> str:
        with self._lock:
            agora = time.time()
            if forcar_novo:
                self._access_exp = 0.0
            if self._access and self._access_exp - 60 > agora:
                return self._access
            if self._refresh and self._refresh_exp - 60 > agora:
                try:
                    d = self._post_token("/api/token/refresh", {"refresh": self._refresh})
                    self._access = d["access"]
                    self._access_exp = _exp_jwt(self._access)
                    return self._access
                except Exception as e:
                    log.warning(f"[OLT Cloud] Refresh do token falhou, logando de novo: {e}")
            self._logar()
            return self._access

    def get(self, caminho: str, params: dict | None = None) -> dict:
        """GET com 1 novo login em 401 e até 3 tentativas em 429/5xx."""
        if not self.configurado():
            raise OltCloudErro("OLTCLOUD_URL/OLTCLOUD_USER/OLTCLOUD_PASSWORD não configurados")
        ultimo_erro = None
        forcar_novo = False
        for tentativa in range(3):
            token = self._token(forcar_novo)
            forcar_novo = False
            try:
                r = self._sessao.get(f"{self.url}{caminho}", params=params,
                                     headers={"Authorization": f"Bearer {token}"}, timeout=self.timeout)
            except requests.RequestException as e:
                ultimo_erro = e
                time.sleep(2 * (tentativa + 1))
                continue
            if r.status_code == 401:
                forcar_novo = True
                ultimo_erro = OltCloudErro(f"{caminho}: 401")
                continue
            if r.status_code in (429, 500, 502, 503, 504):
                ultimo_erro = OltCloudErro(f"{caminho}: {r.status_code}")
                time.sleep(3 * (tentativa + 1))
                continue
            if r.status_code != 200:
                raise OltCloudErro(f"{caminho}: {r.status_code} {r.text[:200]}")
            dados = r.json()
            if isinstance(dados, dict) and dados.get("return") is False:
                raise OltCloudErro(f"{caminho}: {dados.get('error')}")
            return dados
        raise OltCloudErro(f"{caminho}: falhou 3 vezes ({ultimo_erro})")

    def listar(self, caminho: str, params: dict | None = None) -> list[dict]:
        """Todas as páginas de uma listagem, pedidas pelo número da página.

        Não segue o "next" da resposta: a partir da página 3 ele volta com
        page_size=100 no lugar do tamanho pedido (visto em 24/09/2026), o que
        repetiria itens. Deduplica por id no fim, porque a lista pode mudar
        entre uma página e outra.
        """
        base = dict(params or {})
        base["page_size"] = TAMANHO_PAGINA
        primeira = self.get(caminho, {**base, "page": 1})
        itens = list(primeira.get("results") or [])
        total = int(primeira.get("count") or len(itens))
        for pagina in range(2, math.ceil(total / TAMANHO_PAGINA) + 1):
            itens += self.get(caminho, {**base, "page": pagina}).get("results") or []
        unicos = {}
        for it in itens:
            unicos[it.get("id", id(it))] = it
        return list(unicos.values())

    # ── Recursos ──────────────────────────────────────────────
    def equipamentos(self, **filtros) -> list[dict]:
        return self.listar("/api/v2/ftth/equipment/list", filtros)

    def equipamentos_alterados_desde(self, desde: datetime) -> list[dict]:
        """ONUs cujo status OU sinal mudou desde `desde` (1 ou 2 páginas por minuto)."""
        ts = desde.strftime(FMT_DATA)
        por_id = {}
        for campo in ("last_status_update", "last_signal_update"):
            for e in self.equipamentos(**{campo: ts, "oper": ">="}):
                por_id[e["id"]] = e
        return list(por_id.values())

    def equipamentos_fora_do_ar(self) -> list[dict]:
        por_id = {}
        for status in STATUS_LEGADO:
            if status == "Online":
                continue
            for e in self.equipamentos(status=status, oper="="):
                por_id[e["id"]] = e
        return list(por_id.values())

    def pons(self) -> list[dict]:
        return self.listar("/api/v2/ftth/pons")

    def equipamento(self, id_equipamento) -> dict:
        return self.get(f"/api/v2/ftth/equipment/{id_equipamento}").get("equipment") or {}

    def status_logs(self, id_equipamento) -> list[dict]:
        return self.get(f"/api/v2/ftth/equipment/{id_equipamento}/status_logs").get("results") or []

    def buscar_por_serial(self, sn: str) -> dict | None:
        alvo = (sn or "").strip().upper()
        if not alvo:
            return None
        for e in self.equipamentos(serial_number=alvo, oper="="):
            if (e.get("serial_number") or "").upper() == alvo:
                return e
        return None


# ── Quedas por PON (substitui o get_outage_pons do SmartOLT) ──
# Regras calibradas contra o get_outage_pons do SmartOLT em 24/09/2026 (16:50),
# no mesmo instante, com o status das ONUs no OLT Cloud:
# - Queda total: TODAS as ONUs da PON fora. O SmartOLT chamava de "power"
#   (todas sem energia) ou "los" (todas sem sinal) enquanto a queda era recente,
#   e de "offline" quando já passava de dias (is_active=0 na resposta dele).
#   O mais recente ainda "power" tinha 29 h; o mais antigo já "offline", 8 dias.
# - LOS parcial: um GRUPO de ONUs da mesma PON perdendo sinal junto. Caso real:
#   7 ONUs de 61 em TAGUATINGA-N1 9/9 caíram no mesmo segundo; nas outras 557
#   PONs com ONU fora, o maior grupo simultâneo foi 2. ONU caída sozinha (ex-
#   cliente, cabo do próprio cliente) não é queda de PON.
# Lidos na hora do cálculo, não na importação: o otdr_alertas.py importa este
# módulo antes de carregar o .env.
def _limiares() -> tuple[timedelta, int, timedelta]:
    return (timedelta(minutes=int(os.getenv("OTDR_PARCIAL_JANELA_MIN") or 10)),
            int(os.getenv("OTDR_PARCIAL_MIN_ONUS") or 3),
            timedelta(hours=int(os.getenv("OTDR_QUEDA_ATIVA_HORAS") or 72)))


def _maior_grupo_simultaneo(onus: list[dict], janela: timedelta) -> list[dict]:
    com_data = sorted((o for o in onus if _data(o.get("last_status_change"))),
                      key=lambda o: _data(o["last_status_change"]))
    melhor, inicio = [], 0
    for fim in range(len(com_data)):
        while _data(com_data[fim]["last_status_change"]) - _data(com_data[inicio]["last_status_change"]) > janela:
            inicio += 1
        if fim - inicio + 1 > len(melhor):
            melhor = com_data[inicio:fim + 1]
    return melhor


def chave_pon(onu: dict) -> tuple[str, str, str]:
    return (onu.get("olt_name") or "", _texto(onu.get("board")), _texto(onu.get("port")))


def totais_por_pon(pons: list[dict]) -> dict[tuple[str, str, str], int]:
    """Total de ONUs por PON a partir da listagem de PONs do OLT Cloud."""
    return {(p.get("olt_name") or "", _texto(p.get("slot")), _texto(p.get("pon"))): int(p.get("onu_count") or 0)
            for p in pons}


def calcular_outages(onus: list[dict], totais: dict | None = None, agora: datetime | None = None) -> list[dict]:
    """PONs em queda, no formato de uma porta do get_outage_pons do SmartOLT,
    com a chave "categoria" (partial_los | los | power | offline) que o detector
    e o dashboard já liam.

    `onus` no formato SmartOLT (para_formato_smartolt). Pode ser a rede inteira
    (dashboard) ou só as ONUs fora do ar (detector); no segundo caso, `totais`
    precisa vir da listagem de PONs, senão toda PON pareceria 100% fora.
    """
    agora = agora or datetime.now()
    janela_parcial, parcial_min_onus, queda_ativa = _limiares()
    por_pon = defaultdict(list)
    for o in onus:
        por_pon[chave_pon(o)].append(o)
    if totais is None:
        totais = {k: len(v) for k, v in por_pon.items()}

    portas = []
    for chave, lista in por_pon.items():
        fora = [o for o in lista if o.get("status") != "Online"]
        if not fora:
            continue
        total = max(int(totais.get(chave) or 0), len(lista))
        los = [o for o in fora if o.get("status") == "LOS"]
        energia = [o for o in fora if o.get("status") == "Power fail"]
        base = {
            "olt_id":     fora[0].get("olt_id"),
            "olt_name":   chave[0],
            "board":      chave[1],
            "port":       chave[2],
            "zone_name":  ZONA_LEGADA,
            "total_onus": total,
        }

        if len(fora) >= total:
            ultima = max((_data(o.get("last_status_change")) for o in fora), default=None, key=lambda d: d or datetime.min)
            if not ultima or agora - ultima > queda_ativa:
                categoria = "offline"
            elif len(energia) == len(fora):
                categoria = "power"
            elif len(los) == len(fora):
                categoria = "los"
            else:
                categoria = "offline"
            portas.append({
                **base,
                "categoria":            categoria,
                "alert_kind":           "full_outage",
                "is_active":            0 if categoria == "offline" else 1,
                "affected_onus":        total,
                "affected_percent":     100.0,
                "los_count":            len(los),
                "power_count":          len(energia),
                "offline_count":        len(fora) - len(los) - len(energia),
                "latest_status_change": ultima.strftime(FMT_DATA) if ultima else None,
            })
            continue

        grupo = _maior_grupo_simultaneo(los, janela_parcial)
        if len(grupo) >= parcial_min_onus:
            inicio = _data(grupo[0]["last_status_change"])
            portas.append({
                **base,
                "categoria":            "partial_los",
                "alert_kind":           "partial_los",
                "is_active":            1,
                "affected_onus":        len(grupo),
                "affected_percent":     round(100 * len(grupo) / total, 1),
                "los_count":            len(grupo),
                "power_count":          len(energia),
                "offline_count":        len(fora) - len(grupo) - len(energia),
                "latest_status_change": inicio.strftime(FMT_DATA),
                "partial_started_at":   inicio.strftime(FMT_DATA),
                "partial_last_seen_at": agora.strftime(FMT_DATA),
            })
    return portas


def outage_pons(cliente: OltCloud, agora: datetime | None = None) -> list[dict]:
    """Quedas de PON da rede inteira pedindo só o necessário: as ONUs fora do
    ar (3 listagens por status) e o total de ONUs por PON (listagem de PONs).
    ~5 requisições por ciclo, contra 16 páginas da rede inteira."""
    fora = [para_formato_smartolt(e) for e in cliente.equipamentos_fora_do_ar()]
    return calcular_outages(fora, totais_por_pon(cliente.pons()), agora)


# ── Detalhe de uma ONU (substitui o get_onu_full_status_info) ─
def detalhe_onu(cliente: OltCloud, sn: str, max_eventos: int = 15) -> dict | None:
    """Estado da ONU e histórico de status, para a análise de causa provável.
    None quando o serial não existe no OLT Cloud."""
    eq = cliente.buscar_por_serial(sn)
    if not eq:
        return None
    det = cliente.equipamento(eq["id"])
    eventos = cliente.status_logs(eq["id"])[:max_eventos]
    status = STATUS_LEGADO.get(det.get("status"), det.get("status"))
    online = status == "Online"
    return {
        "sn":             (det.get("serial_number") or sn).upper(),
        "status":         status,
        "rx_onu":         _sinal(det.get("device_rx")) if online else None,
        "rx_olt":         _sinal(det.get("olt_rx")) if online else None,
        "distancia_m":    det.get("optical_module_distance") or det.get("distance"),
        "temperatura":    det.get("optical_module_temperature"),
        "tensao":         det.get("optical_module_volt"),
        "corrente_ma":    det.get("optical_module_current"),
        "intermitencia":  bool(det.get("optical_module_intermittency")),
        "ultimo_alarme":  det.get("optical_module_last_alarm"),
        "ultima_queda":   _txt_data(det.get("last_disconnection")),
        "online_desde":   _txt_data(det.get("uptime_since")),
        "eventos":        [{"data": _txt_data(e.get("date")), "status": STATUS_LEGADO.get(e.get("status"), e.get("status"))}
                           for e in eventos],
    }
