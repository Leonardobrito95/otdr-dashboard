# OTDR Dashboard

Monitoramento de mais de 17.000 ONUs de fibra óptica em tempo real, cruzando a API da SmartOLT com o billing (IXC) e um histórico próprio, com alertas autônomos na degradação de sinal.

## O problema que isso resolve

Uma queda de fibra ou de energia num POP afeta dezenas ou centenas de clientes de uma vez, mas a informação nasce espalhada: o status da ONU está na SmartOLT, o cadastro e o contrato estão no IXC, e ninguém tinha um jeito de olhar as duas coisas juntas em tempo real nem de saber, na hora, se a causa era queda de energia ou rompimento de fibra — a diferença muda completamente o que time interno e cliente final precisam ouvir.

## O que o sistema faz

Três processos de coleta e um dashboard Flask:

- **`otdr_alertas.py`** consulta a SmartOLT a cada 7 minutos, detecta novas quedas por porta PON e por saúde geral da OLT, decide a causa dominante do evento (energia vs. fibra vs. offline) a partir da proporção de ONUs afetadas por cada motivo, e dispara e-mail interno e aviso ao cliente final (via SYNKR) só quando o evento é realmente relevante — uma oscilação breve de energia não gera aviso ao cliente.
- **`otdr_snapshot.py`** e **`otdr_smartolt.py`** rodam snapshots diários (IXC → Postgres e SmartOLT → Postgres), construindo o histórico que alimenta as telas de evolução e piora.
- **`dashboard/app.py`** expõe o painel web: visão geral, mapa de POPs, saúde por OLT, histórico e evolução, KPIs, consulta de cliente com causa provável de problema, e o painel de OS.

## Decisões de engenharia que valem destacar

- **Causa dominante, não hedge:** em vez de listar as três causas possíveis toda vez, o sistema calcula qual causa (energia, fibra, offline) responde pela maior fatia das ONUs afetadas e comunica isso — internamente e ao cliente.
- **Honestidade com o cliente final:** durante uma queda de energia pura, o texto do aviso não promete "equipe já está atuando" — não há o que uma equipe de fibra faça por uma queda de energia, e prometer isso gera expectativa errada.
- **Limite configurável pro aviso ao cliente:** eventos de energia abaixo de um percentual mínimo de ONUs afetadas (padrão 40%, via env) não geram notificação ao cliente, só fica registrado internamente — evita ruído em blips curtos que se resolvem sozinhos.
- **Fuso horário validado ao vivo:** o timestamp que a SmartOLT retorna já vem em horário de Brasília, não em UTC — confirmado comparando o "há quanto tempo" mostrado pela própria SmartOLT com o horário real do evento.

## Stack

| Camada | Tecnologia |
|---|---|
| Coleta | Python (requests contra a API da SmartOLT, mysql-connector contra o IXC) |
| Dashboard | Flask |
| Banco | PostgreSQL (histórico e estado) |
| Alertas | SMTP (e-mail interno) e SYNKR (aviso ao cliente final) |
| Execução | systemd (serviço contínuo de detecção + snapshots diários agendados) |

## Rodando localmente

```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env          # preencha SmartOLT, IXC (MySQL), Postgres, SMTP e demais chaves
python otdr_snapshot.py       # snapshot inicial: IXC -> Postgres
python otdr_smartolt.py       # snapshot inicial: SmartOLT -> Postgres
python otdr_alertas.py        # detector de queda em tempo real (loop contínuo)
python dashboard/app.py       # painel web, http://localhost:5008
```

Em produção, cada processo roda como serviço systemd independente — `otdr_alertas.py` contínuo, os dois snapshots agendados diariamente.

<sub>Sistema interno de uma ISP regional. Credenciais e dados reais foram removidos desta versão de portfólio.</sub>
