# 📡 OTDR Dashboard — Monitoramento de Fibra Óptica

> Dashboard interno que monitora **17.000+ ONUs** de uma rede de fibra óptica em tempo real, cruzando a API da SmartOLT com o billing (IXC) e um histórico próprio em PostgreSQL — com **alertas autônomos** quando a qualidade do sinal degrada.

![Python](https://img.shields.io/badge/Python-3.11+-blue?logo=python&logoColor=white)
![Flask](https://img.shields.io/badge/Flask-dashboard-000000?logo=flask&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-histórico-336791?logo=postgresql&logoColor=white)
![MySQL](https://img.shields.io/badge/MySQL-IXC-4479A1?logo=mysql&logoColor=white)

---

## 🎯 O problema

Numa ISP, a saúde da rede de fibra vive na plataforma da OLT (SmartOLT), mas os dados de cliente/contrato vivem no ERP (IXC). Para saber *"quais clientes estão com sinal crítico na OLT de Águas Claras?"* alguém precisava cruzar dois sistemas na mão. E quando o sinal de uma ONU caía, ninguém sabia — até o cliente ligar.

## 💡 A solução

Um dashboard que unifica as duas fontes e adiciona uma camada de histórico e alerta:

- **Visão em tempo real** das 17.000+ ONUs: nível de sinal (dBm), status (OK / Crítico / Fora de Operação), LOS e falhas de energia.
- **Cruzamento OLT × IXC** — cada ONU é associada ao cliente/contrato, com filtro por OLT, região e faixa de sinal.
- **Histórico em PostgreSQL** — snapshots periódicos permitem ver a *degradação ao longo do tempo*, não só o instante atual.
- **Alertas autônomos por e-mail** — quando uma OLT ultrapassa um % de ONUs críticas, dispara alerta (com cooldown anti-spam).

---

## 🧩 Componentes

| Arquivo | Papel |
|---|---|
| `dashboard/app.py` | Aplicação Flask — API + telas (KPIs, mapa, OS, histórico) |
| `otdr_smartolt.py` | Cliente da API SmartOLT — coleta o estado das ONUs |
| `otdr_snapshot.py` | Job de snapshot — grava o estado periódico no PostgreSQL |
| `otdr_alertas.py` | Motor de alertas autônomos — avalia thresholds e envia e-mail |

## 🛠️ Stack

| Camada | Tecnologia |
|---|---|
| Backend | Python + **Flask** |
| Fontes | **SmartOLT API** (REST) + **MySQL** (IXC) |
| Histórico | **PostgreSQL** |
| Alertas | SMTP (e-mail) com cooldown |
| Coleta | Threading + polling configurável |

---

## 📊 O que o dashboard mostra

- **KPIs** — total de ONUs, % crítico, fora de operação, por OLT
- **Mapa** — distribuição geográfica dos problemas
- **OS** — ordens de serviço relacionadas a falhas de rede
- **Histórico** — evolução do sinal e dos incidentes no tempo

---

## 🚀 Rodando localmente

```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env          # preencha SmartOLT, MySQL e PostgreSQL
python dashboard/app.py       # http://localhost:5008
```

Para o monitoramento autônomo:
```bash
python otdr_snapshot.py &     # coleta periódica
python otdr_alertas.py &      # alertas por e-mail
```

---

<sub>Dashboard interno de uma ISP regional. Dados reais (cache de ONUs, credenciais) foram removidos desta versão de portfólio.</sub>
