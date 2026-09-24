"""Testes do cliente do OLT Cloud (python -m unittest discover -s tests)."""
import base64
import json
import sys
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import oltcloud  # noqa: E402

FIXTURE = json.loads((Path(__file__).parent / "fixtures" / "outage_20260924.json").read_text())


def _jwt(exp: float) -> str:
    corpo = base64.urlsafe_b64encode(json.dumps({"exp": exp}).encode()).decode().rstrip("=")
    return f"cabecalho.{corpo}.assinatura"


def _resposta(status=200, corpo=None):
    r = mock.Mock()
    r.status_code = status
    r.json.return_value = corpo if corpo is not None else {}
    r.text = json.dumps(corpo or {})
    return r


class TestFormatoSmartolt(unittest.TestCase):
    EQ = {
        "id": 7, "serial_number": "zteGd82efb49", "status": "Online", "olt": "AGUAS CLARAS-1", "olt_id": 3,
        "slot": 2, "pon": 10, "onu_id": 1, "model": "F6600PV9.0.12", "olt_rx": -26.235, "device_rx": -22.678,
        "external_client_id": "38673", "device_alias": "38673 - FULANO", "last_status_update": "2026-09-18T11:57:18.001648",
    }

    def test_campos_no_formato_do_smartolt(self):
        o = oltcloud.para_formato_smartolt(self.EQ)
        self.assertEqual(o["sn"], "ZTEGD82EFB49")
        self.assertEqual(o["olt_id"], "18")  # id do SmartOLT, não o 3 do OLT Cloud
        self.assertEqual((o["board"], o["port"], o["onu"]), ("2", "10", "1"))
        self.assertEqual(o["signal_1310"], "-26.235")  # RX na OLT
        self.assertEqual(o["signal_1490"], "-22.678")  # RX na ONU
        self.assertEqual(o["signal"], "Warning")
        self.assertEqual(o["name"], "id38673")
        self.assertEqual(o["zone_name"], "CANAA")
        self.assertEqual(o["last_status_change"], "2026-09-18 11:57:18")

    def test_status_traduzido_e_onu_fora_do_ar_sem_sinal(self):
        for novo, legado in (("Sem Energia", "Power fail"), ("LOSS", "LOS"), ("Inativo", "Offline")):
            o = oltcloud.para_formato_smartolt({**self.EQ, "status": novo})
            self.assertEqual(o["status"], legado)
            self.assertIsNone(o["signal_1310"])  # o OLT Cloud guarda a leitura velha; não pode passar
            self.assertIsNone(o["signal"])

    def test_snapshot_diario_mantem_a_ultima_leitura_da_onu_fora_do_ar(self):
        o = oltcloud.para_formato_smartolt({**self.EQ, "status": "Sem Energia"}, manter_ultimo_sinal=True)
        self.assertEqual((o["status"], o["signal_1310"], o["signal"]), ("Power fail", "-26.235", "Warning"))
        vazio = oltcloud.para_formato_smartolt({**self.EQ, "status": "LOSS", "olt_rx": -99.99}, manter_ultimo_sinal=True)
        self.assertIsNone(vazio["signal_1310"])

    def test_leitura_invalida_vira_vazio(self):
        o = oltcloud.para_formato_smartolt({**self.EQ, "olt_rx": -99.99, "device_rx": -99.99})
        self.assertIsNone(o["signal_1310"])
        self.assertIsNone(o["signal_1490"])

    def test_olt_nova_e_cliente_sem_vinculo(self):
        o = oltcloud.para_formato_smartolt({**self.EQ, "olt": "OLT NOVA", "olt_id": 99, "external_client_id": None})
        self.assertEqual(o["olt_id"], "oc99")
        self.assertEqual(o["name"], "38673 - FULANO")

    def test_faixas_da_classe_de_sinal(self):
        self.assertEqual(oltcloud.classe_sinal(-25.995), "Very good")
        self.assertEqual(oltcloud.classe_sinal(-26.003), "Warning")
        self.assertEqual(oltcloud.classe_sinal(-29.981), "Warning")
        self.assertEqual(oltcloud.classe_sinal(-30.0), "Critical")
        self.assertIsNone(oltcloud.classe_sinal(None))


class TestQuedasPorPon(unittest.TestCase):
    def test_reproduz_o_get_outage_pons_do_smartolt_do_mesmo_instante(self):
        onus = [oltcloud.para_formato_smartolt(e) for e in FIXTURE["onus_fora"]]
        agora = datetime.strptime(FIXTURE["agora"], "%Y-%m-%d %H:%M:%S")
        portas = oltcloud.calcular_outages(onus, oltcloud.totais_por_pon(FIXTURE["pons"]), agora)
        obtido = sorted((p["olt_name"], p["board"], p["port"], p["categoria"]) for p in portas)
        esperado = sorted((p["olt_name"], p["board"], p["port"], p["categoria"]) for p in FIXTURE["smartolt"])
        self.assertEqual(obtido, esperado)
        parcial = next(p for p in portas if p["categoria"] == "partial_los")
        self.assertEqual((parcial["affected_onus"], parcial["total_onus"], parcial["affected_percent"]), (7, 61, 11.5))

    def _onu(self, status, quando, board="1", port="1"):
        return {"olt_id": "18", "olt_name": "AGUAS CLARAS-1", "board": board, "port": port, "status": status,
                "last_status_change": quando}

    def test_queda_total_recente_por_energia_e_antiga_vira_offline(self):
        agora = datetime(2026, 9, 24, 12, 0, 0)
        recente = [self._onu("Power fail", "2026-09-24 11:00:00") for _ in range(3)]
        antiga = [self._onu("Power fail", "2026-09-10 11:00:00", port="2") for _ in range(2)]
        portas = {p["port"]: p for p in oltcloud.calcular_outages(recente + antiga, agora=agora)}
        self.assertEqual(portas["1"]["categoria"], "power")
        self.assertEqual(portas["2"]["categoria"], "offline")
        self.assertEqual(portas["2"]["is_active"], 0)

    def test_onus_caidas_em_horarios_diferentes_nao_sao_queda_de_pon(self):
        agora = datetime(2026, 9, 24, 12, 0, 0)
        onus = [self._onu("Online", None) for _ in range(20)]
        onus += [self._onu("LOS", f"2026-09-2{i} 10:00:00") for i in range(1, 4)]
        self.assertEqual(oltcloud.calcular_outages(onus, agora=agora), [])

    def test_limiares_vem_do_ambiente_na_hora_do_calculo(self):
        agora = datetime(2026, 9, 24, 12, 0, 0)
        tres = [self._onu("LOS", "2026-09-24 11:00:00") for _ in range(3)] + [self._onu("Online", None) for _ in range(10)]
        self.assertEqual(len(oltcloud.calcular_outages(tres, agora=agora)), 1)
        with mock.patch.dict("os.environ", {"OTDR_PARCIAL_MIN_ONUS": "4"}):
            self.assertEqual(oltcloud.calcular_outages(tres, agora=agora), [])

    def test_so_as_onus_fora_do_ar_precisam_dos_totais_da_pon(self):
        agora = datetime(2026, 9, 24, 12, 0, 0)
        fora = [self._onu("LOS", "2026-09-24 11:59:00")]
        # sem o total, 1 ONU fora pareceria a PON inteira
        self.assertEqual(oltcloud.calcular_outages(fora, agora=agora)[0]["categoria"], "los")
        self.assertEqual(oltcloud.calcular_outages(fora, {("AGUAS CLARAS-1", "1", "1"): 30}, agora), [])


class TestCliente(unittest.TestCase):
    def _cliente(self):
        c = oltcloud.OltCloud("https://x.oltcloud.co/", "u", "s")
        c._sessao = mock.Mock()
        return c

    def test_pagina_pelo_numero_e_nao_pelo_next(self):
        c = self._cliente()
        paginas = {1: [{"id": i} for i in range(1000)], 2: [{"id": i} for i in range(1000, 2000)], 3: [{"id": 2000}, {"id": 5}]}
        with mock.patch.object(c, "get", side_effect=lambda caminho, p: {"count": 2001, "results": paginas[p["page"]],
                                                                          "next": "http://x/?page=4&page_size=100"}) as g:
            itens = c.listar("/api/v2/ftth/equipment/list", {"status": "LOSS"})
        self.assertEqual([chamada.args[1]["page"] for chamada in g.call_args_list], [1, 2, 3])
        self.assertTrue(all(chamada.args[1]["page_size"] == 1000 and chamada.args[1]["status"] == "LOSS" for chamada in g.call_args_list))
        self.assertEqual(len(itens), 2001)  # o id 5 repetido na página 3 conta uma vez

    def test_renova_token_e_loga_de_novo_depois_de_401(self):
        c = self._cliente()
        agora = 1_800_000_000
        c._sessao.post.side_effect = [
            _resposta(200, {"access": _jwt(agora + 10800), "refresh": _jwt(agora + 86400)}),  # 1º login
            _resposta(200, {"access": _jwt(agora + 10800), "refresh": _jwt(agora + 86400)}),  # login após 401
        ]
        c._sessao.get.side_effect = [_resposta(401), _resposta(200, {"return": True, "results": []})]
        with mock.patch("oltcloud.time.time", return_value=agora):
            self.assertEqual(c.get("/api/v2/ftth/pons"), {"return": True, "results": []})
        self.assertEqual(c._sessao.post.call_count, 2)
        self.assertEqual(c._sessao.post.call_args_list[0].args[0], "https://x.oltcloud.co/api/token")

    def test_usa_refresh_quando_o_access_venceu(self):
        c = self._cliente()
        agora = 1_800_000_000
        c._access, c._access_exp = "velho", agora - 10
        c._refresh, c._refresh_exp = "r", agora + 3600
        c._sessao.post.return_value = _resposta(200, {"access": _jwt(agora + 10800)})
        with mock.patch("oltcloud.time.time", return_value=agora):
            c._token()
        self.assertEqual(c._sessao.post.call_args.args[0], "https://x.oltcloud.co/api/token/refresh")

    def test_erro_da_api_vira_excecao(self):
        c = self._cliente()
        c._access, c._access_exp = "t", 9e12
        c._sessao.get.return_value = _resposta(200, {"return": False, "error": "campo inválido"})
        with self.assertRaises(oltcloud.OltCloudErro):
            c.get("/api/v2/ftth/pons")

    def test_detalhe_onu(self):
        c = self._cliente()
        with mock.patch.object(c, "buscar_por_serial", return_value={"id": 9}), \
             mock.patch.object(c, "equipamento", return_value={"serial_number": "ZTEG1", "status": "Online", "device_rx": -22.6,
                                                                  "olt_rx": -26.2, "optical_module_distance": 1870,
                                                                  "optical_module_last_alarm": "Sem Energia"}), \
             mock.patch.object(c, "status_logs", return_value=[{"date": "2026-09-18 11:57:20", "status": "Online"},
                                                                {"date": "2026-09-18 11:50:53", "status": "Sem Energia"}]):
            d = oltcloud.detalhe_onu(c, "zteg1")
        self.assertEqual(d["distancia_m"], 1870)
        self.assertEqual([e["status"] for e in d["eventos"]], ["Online", "Power fail"])


if __name__ == "__main__":
    unittest.main()
