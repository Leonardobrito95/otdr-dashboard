#!/usr/bin/env python3
"""
Avisos de parada para quedas de serviço de terceiro (WhatsApp, Google, AWS).

Quem detecta a queda é o Canaã Performance (módulo servicosExternos), que
sonda os serviços a partir da nossa rede e lê as status pages oficiais. Quem
fala com o call center da Aprimorar é este arquivo, porque o OTDR já tem as
credenciais do Synkr, já sabe abrir e fechar aviso e já mantém o controle de
qual aviso está aberto. Dois sistemas logando no Synkr criariam duas sessões
e dois estados para o mesmo evento, que é como se produz aviso duplicado.

A ponte entre os dois é o Postgres que ambos já compartilham, no sentido
inverso do que existe hoje: o Canaã Performance lê `otdr.alertas_historico`,
e aqui se lê `diagnostico.servico_externo_queda`. Mesma convenção nas duas
tabelas, `fim IS NULL` significa em curso.

DUAS CATEGORIAS, e a diferença entre elas decide o que o cliente ouve:

  suspeita_rede_local = false
      Um serviço de terceiro caiu. A internet do cliente está boa. O aviso
      serve para o atendente não abrir O.S. nem mandar técnico, e o texto ao
      cliente precisa dizer isso com todas as letras.

  suspeita_rede_local = true
      Serviços de empresas diferentes caíram juntos, então a causa provável é
      a nossa saída. Aqui é parada da Canaã de verdade, mesmo caso de uso da
      queda de PON que este sistema já avisa.
"""

import logging
import psycopg2
import psycopg2.extras

log = logging.getLogger(__name__)

# Prefixo próprio na chave do aviso: o controle de avisos abertos
# (synkr_avisos.json) é compartilhado com os alertas de PON, e sem isso um
# serviço chamado "netflix" poderia colidir com alguma chave de OLT.
PREFIXO_CHAVE = "servico:"


def _texto_para_cliente(queda: dict) -> str:
    """O que o atendente lê para quem ligou.

    Na queda de terceiro a mensagem mais valiosa é a segunda frase: sem ela o
    cliente desliga achando que a internet dele tem problema, e liga de novo
    amanhã.
    """
    if queda["suspeita_rede_local"]:
        return ("Identificamos uma instabilidade no acesso à internet na sua região. "
                "Nossa equipe técnica já está atuando na correção. "
                "Pedimos desculpas pelo transtorno.")
    return (f"O serviço {queda['nome']} está apresentando instabilidade no momento, "
            "e isso afeta usuários de várias operadoras, não apenas a nossa. "
            "Sua conexão está funcionando normalmente e nenhum reparo é necessário.")


def _descricao_interna(queda: dict) -> str:
    """Texto técnico, para o call center entender o que está acontecendo."""
    origem = ("sonda a partir da nossa rede" if queda["fonte"] == "sonda_propria"
              else "status publicado pela própria empresa")
    if queda["suspeita_rede_local"]:
        return (f"Serviços de empresas diferentes fora ao mesmo tempo (o gatilho foi {queda['nome']}). "
                "Quedas simultâneas de empresas independentes são raras: a causa provável é a saída "
                "da Canaã, não o serviço. Detectado pelo Canaã Performance por " + origem + ".")
    return (f"{queda['nome']} ({queda['dono']}) sem resposta desde {queda['inicio']:%H:%M}. "
            f"Detectado por {origem}. A rede da Canaã não está em falha por isso: "
            "não abrir O.S. nem acionar campo por reclamação ligada a este serviço.")


def _impacto(queda: dict) -> str:
    if queda["suspeita_rede_local"]:
        return "Potencialmente toda a base, enquanto durar."
    return f"Clientes que usam {queda['nome']}. A conexão em si não está afetada."


def sincronizar_avisos(pg_config: dict, criar_aviso, fechar_aviso) -> None:
    """Abre aviso para queda nova e fecha o das que já normalizaram.

    `criar_aviso` e `fechar_aviso` são as funções que já existem no
    otdr_alertas.py, recebidas como parâmetro em vez de importadas: evita
    import circular e deixa este arquivo testável sem subir o OTDR inteiro.

    Nunca levanta exceção. Falha aqui não pode derrubar o ciclo de alertas de
    PON, que é a função principal do sistema.
    """
    try:
        with psycopg2.connect(**pg_config) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT id, servico, nome, dono, fonte, rotulo, inicio,
                           suspeita_rede_local, avisado_em
                    FROM diagnostico.servico_externo_queda
                    WHERE fim IS NULL
                    ORDER BY inicio
                """)
                em_curso = cur.fetchall()

                # Avisos que este arquivo abriu e cujo episódio já fechou do
                # outro lado. Fechar pela ausência, e não por uma flag, evita
                # aviso órfão quando o backend reinicia no meio de uma queda.
                cur.execute("""
                    SELECT servico, nome
                    FROM diagnostico.servico_externo_queda
                    WHERE fim IS NOT NULL AND avisado_em IS NOT NULL
                      AND fim > now() - interval '1 hour'
                """)
                normalizadas = cur.fetchall()

                for q in em_curso:
                    if q["avisado_em"] is not None:
                        continue
                    chave = PREFIXO_CHAVE + q["servico"]
                    criar_aviso(
                        chave,
                        description=_descricao_interna(q),
                        impact=_impacto(q),
                        text_for_client=_texto_para_cliente(q),
                        start_dt=q["inicio"],
                    )
                    cur.execute(
                        "UPDATE diagnostico.servico_externo_queda SET avisado_em = now() WHERE id = %s",
                        (q["id"],),
                    )
                    log.info(f"[SERVICOS] Aviso de parada aberto para {q['nome']}")

                for q in normalizadas:
                    chave = PREFIXO_CHAVE + q["servico"]
                    fechar_aviso(chave, f"{q['nome']} normalizado.")

    except Exception as e:
        log.error(f"[SERVICOS] Falha ao sincronizar avisos de serviço externo: {e}")
