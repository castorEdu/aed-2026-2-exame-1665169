# ADR-006 — Resiliência no servico-estoque: retentativa, DLQ e compensação

## Status

Aceita · 2026-10-26 · Exame especial

## Contexto

Até o código base, o `servico-estoque` decidia o que fazer com uma falha por omissão, pelo padrão do Spring Kafka. Duas coisas estavam erradas:

- um SKU inexistente passava em despercebido: o `UPDATE` de `reservar()` afetava zero linhas, não lançava nada, e o evento era marcado como processado;
- qualquer outra falha era retentada pelo padrão do framework, sem que ninguém tivesse escolhido o limite, a espera ou o destino final.

Além disso, o caso do Mercado Rápido tem um caminho de exceção que o código não conhecia: o pedido é confirmado, o estoque é reservado e o pagamento é recusado. A reserva já aconteceu e outro consumidor pode já tê-la lido, então ela não se apaga: desfaz-se com um fato novo.

Este ADR registra as decisões que tornam esses dois caminhos explícitos.

## Decisão

### 1. Classificação das falhas

| Classe | O que é | Exemplos no código | Tratamento |
|---|---|---|---|
| Permanente | tentar de novo dá o mesmo erro, porque o defeito está no evento ou nos dados | `SkuInexistenteException`, `QuantidadeInvalidaException`, `EventoMalformadoException` (JSON ilegível ou `ce_id` ausente), todas filhas de `FalhaPermanenteException` | não retenta, vai direto à DLQ |
| Transitória | o evento está certo e o ambiente está momentaneamente indisponível | Postgres parado ou reiniciando: `CannotCreateTransactionException`, `DataAccessResourceFailureException`, `TransientDataAccessException`, `SQLTransientException` | retenta com espera exponencial e limite |
| Inesperada | qualquer exceção que não é nenhuma das duas | defeito de código, erro de integridade não previsto | retenta com espera fixa e curta, e depois DLQ |

A permanente é reconhecida por tipo, em [`FalhaPermanenteException`](../../servico-estoque/src/main/java/br/pucminas/aed/estoque/domain/FalhaPermanenteException.java), e registrada em `addNotRetryableExceptions` na [`ResilienciaConfig`](../../servico-estoque/src/main/java/br/pucminas/aed/estoque/config/ResilienciaConfig.java). A transitória é identificada pelo [`ClassificadorDeFalhas`](../../servico-estoque/src/main/java/br/pucminas/aed/estoque/service/ClassificadorDeFalhas.java), que percorre a cadeia de causas, porque o Spring embrulha a exceção original em `ListenerExecutionFailedException`.

A exceção que não se encaixa em nenhuma classe cai em "inesperada" e é retentada, e não mandada direto para a DLQ. O raciocínio é a assimetria do erro: classificar como permanente algo que era transitório perde a recuperação automática e enche a DLQ; classificar como transitório algo que era permanente custa três segundos de espera antes de ir para a DLQ do mesmo jeito.

Um SKU ausente ou uma quantidade menor ou igual a 0 também são falhas permanentes dentro da transação: a exceção desfaz o registro em `evento_processado`. Sem isso, o evento ficaria marcado como visto e o reprocessamento depois da correção seria descartado como duplicata.

### 2. Retentativa: quantas vezes, com que espera, e por quê

O limite é de 3 retentativas depois da entrega original, ou seja, no máximo 4 execuções do mesmo evento.

| Falha | `BackOff` | Esperas | Espera total |
|---|---|---|---|
| Transitória | `ExponentialBackOff`, 2 s inicial, ×2, teto de 10 s | 2 s, 4 s, 8 s | 14 s |
| Inesperada | `FixedBackOff`, 1 s | 1 s, 1 s, 1 s | 3 s |
| Permanente | nenhum | — | 0 |

Com o `connection-timeout` do Hikari em 2 s, cada execução contra o Postgres parado consome mais 2 s. A janela real de uma queda do banco é de cerca de 22 s, e os valores moram em `demo.retentativa.*` no `application.yml`.

Por que esses números:

- O número, 3. Uma queda curta do banco (reinício do contêiner, failover) se resolve em segundos. Se o banco não voltou em 22 s, a quarta tentativa também falharia, e a pergunta deixa de ser "tentar de novo?" e passa a ser "quem cuida disso?". O limite existe porque o consumidor é sequencial por partição: enquanto um evento é retentado, todos os que estão atrás dele na mesma partição esperam. Retentativa sem limite transforma um evento ruim em uma partição parada.
- A espera exponencial para a transitória. Indisponibilidade costuma ser sobrecarga ou reinício, e martelar um banco que está subindo piora a recuperação. Dobrar a espera dá tempo ao banco sem alongar demais o bloqueio.
- A espera fixa e curta para a inesperada. Se for defeito de código, esperar mais não ajuda; a DLQ guarda o evento sem manter a partição presa.
- O teto de 10 s e o timeout de 2 s. Mantêm a janela total bem abaixo do `max.poll.interval.ms` do consumidor (5 min), então o grupo não é considerado morto e não rebalanceia no meio de uma retentativa.

Duas decisões acompanham a política: `setCommitRecovered(true)`, para que o offset do evento enviado à DLQ seja confirmado (o `ack` é manual e, sem isso, um reinício reentregaria um evento que já está na DLQ); e um `RetryListener` ([`RegistroDeFalhas`](../../servico-estoque/src/main/java/br/pucminas/aed/estoque/config/RegistroDeFalhas.java)) que registra cada falha e o envio à DLQ, porque sem ele a falha não aparece no terminal.

### 3. A DLQ e o que ela carrega

O destino é `loja.pedido.confirmado.v1.dlq`, com as mesmas 3 partições do tópico de origem, e o evento chega com:

- a mesma chave, o mesmo corpo e todos os cabeçalhos `ce_*` originais, `ce_id` inclusive, porque é a chave de deduplicação de quem reprocessar;
- `dlq_excecao`, `dlq_motivo` e `dlq_falhou_em` (ISO-8601), escolhidos para serem legíveis a olho nu;
- os cabeçalhos `kafka_dlt-*` do `DeadLetterPublishingRecoverer`: tópico, partição e offset de origem, grupo, e a exceção com stacktrace.

Para o corpo chegar intacto à DLQ, o consumidor passou a receber o JSON como `String` e a lê em [`PedidoConfirmadoLeitor`](../../servico-estoque/src/main/java/br/pucminas/aed/estoque/service/PedidoConfirmadoLeitor.java). Com o `JsonDeserializer`, a DLQ regravaria só os quatro campos que a classe do estoque declara e perderia `clienteId`, `valorTotal`, `moeda` e `preco`, e o reprocessamento reinjetaria um evento diferente do original.

### 4. Como o evento sai da DLQ e volta ao fluxo

O caminho é manual e deliberado: depois de corrigida a causa, roda-se `java -jar servico-estoque-1.0.jar --demo.reprocessar-dlq=true`, implementado em [`DlqReprocessador`](../../servico-estoque/src/main/java/br/pucminas/aed/estoque/service/DlqReprocessador.java). Ele lê a DLQ a partir do último offset confirmado do grupo `estoque-dlq-reprocessador`, republica cada evento em `loja.pedido.confirmado.v1` preservando a chave e os `ce_*`, descarta os cabeçalhos `dlq_*` e `kafka_dlt-*`, e encerra. Rodar de novo não republica o que já foi republicado, e se o evento falhar de novo ele volta para a DLQ.

Reprocessar duas vezes não debita duas vezes porque o evento reentra pelo fluxo normal, onde a deduplicação por `ce_id` em `evento_processado` e o efeito de negócio estão no mesmo commit.

Em produção, quem faria isso é a equipe dona do `servico-estoque`, de plantão, seguindo um roteiro: um alerta dispara quando a DLQ tem mensagem, a pessoa lê `dlq_excecao` e `dlq_motivo`, corrige a causa (cadastrar o SKU, restaurar o banco, corrigir o dado) e só então roda o reprocessamento, como um job pontual com registro de quem executou. Reprocessar antes de corrigir só devolve o evento à DLQ.

### 5. Compensação: coreografada, e não orquestrada

Quando o pagamento é recusado, o `servico-estoque` consome `PagamentoRecusado` (`loja.pagamento.recusado.v1`), devolve exatamente o que reservou para aquele pedido e publica `ReservaDeEstoqueCancelada` (`loja.estoque.reservacancelada.v1`). A decisão tem estas partes:

- O estoque registra o que reservou. Sem uma tabela com pedido, SKU e quantidade, ele não saberia o que devolver. A linha da reserva nunca é apagada: ela ganha `devolvida_em`, e a sequência do que aconteceu (reservou, depois devolveu) fica registrada.
- A compensação é um fato novo. O nome segue a regra de sempre: particípio, linguagem do domínio, fato consumado. Não é `DesfazerReserva`, que é comando, nem `ReservaDeletada`, que é vocabulário de banco.
- Nenhuma chamada HTTP entre os serviços. O desfazer viaja como evento.
- Idempotência em duas camadas. O `ce_id` da recusa vai para `evento_processado`; e mesmo que chegue uma segunda recusa do mesmo pedido com outro `ce_id`, só se devolve o que ainda não tem `devolvida_em`, então a quantidade volta uma vez só.
- Coreografia. O `servico-estoque` reage a um fato publicado por quem sabe do pagamento. Não existe um orquestrador que mande o estoque desfazer algo.

### 6. Fechando a janela entre devolver e publicar

A compensação tem duas escritas em sistemas diferentes: o banco (devolver a quantidade) e o Kafka (publicar `ReservaDeEstoqueCancelada`). Nenhuma transação cobre as duas. A decisão é registrar no mesmo commit da devolução um `cancelamento_evento_id` (o `ce_id` que o evento terá) e deixar `cancelamento_publicado_em` nulo; este só é preenchido depois que o broker confirma a publicação. Se o evento da recusa for reentregue e já constar como processado, o serviço não descarta às cegas: se houver reserva devolvida cujo `cancelamento_publicado_em` seja nulo, ele publica o evento com o mesmo `cancelamento_evento_id` e então marca a publicação. É uma caixa de saída mínima, que mora na mesma transação do efeito. O detalhamento está na resposta à pergunta 1.

## Alternativas consideradas

- Retentativa sem limite. É o comportamento que o enunciado chama de insuficiente. Descartada porque um evento permanentemente inválido, ou um banco fora por horas, prenderia a partição inteira e nada avisaria ninguém.
- Tudo para a DLQ, sem retentativa. Descartada porque uma oscilação de segundos no Postgres encheria a DLQ de eventos corretos e transformaria um solavanco em trabalho manual.
- Consumidor que reprocessa a DLQ automaticamente. Descartada porque a falha permanente voltaria para o fluxo, falharia, voltaria para a DLQ, e assim indefinidamente. A DLQ é o lugar onde alguém decide, e por isso o reprocessamento exige a correção da causa antes.
- Tópicos de retentativa não bloqueantes (`@RetryableTopic`). Evitam a partição parada, mas multiplicam tópicos, quebram a ordem por chave e acrescentam uma peça de infraestrutura que esta escala não justifica.
- Compensação orquestrada. Um serviço de saga mandaria `DesfazerReserva` ao estoque, o que é um comando, exige uma máquina de estados e acopla o orquestrador ao contrato do estoque. Para uma compensação de um único passo, o custo não compensa. Reabre-se a discussão se o desfazer ganhar mais passos (estornar o pagamento, avisar o cliente, devolver crédito) e passar a precisar de ordem e de um lugar onde ver o estado da saga.
- Outbox completo com relay. Resolveria a janela entre devolver e publicar de forma geral, com um processo que lê a tabela e publica. Descartado por ora: é o dobro de peças para o caso de um único evento de saída. A coluna `cancelamento_publicado_em` dá a mesma garantia para este fluxo, e a evolução para um outbox de verdade fica aberta.

## Consequências aceitas

Negativas, aceitas:

- Uma queda do banco maior que cerca de 22 s manda eventos corretos para a DLQ, e alguém precisa reprocessá-los à mão. Foi o preço de não prender a partição indefinidamente.
- Bloqueio por partição durante a retentativa. Um evento transitoriamente falho segura todos os que estão atrás dele na mesma partição por até ~22 s. Com o banco fora do ar e tráfego contínuo, cada evento percorre o ciclo inteiro antes de ir para a DLQ, e o consumo cai para aproximadamente um evento a cada 22 s por partição, até o banco voltar.
- O evento que vai para a DLQ perde a ordem. Ele é reprocessado depois dos que passaram à frente, e o consumidor não pode presumir ordem estrita por pedido nessa situação.
- Espera fixa para falha inesperada gasta 3 s com defeitos determinísticos. É o custo de não saber, a priori, que a exceção é permanente.
- A publicação de `ReservaDeEstoqueCancelada` é at-least-once. Se o serviço cair depois de publicar e antes de marcar `cancelamento_publicado_em`, o evento sai duas vezes, com o mesmo `ce_id`. Quem um dia consumir esse tópico terá de deduplicar por `ce_id`, como o próprio estoque faz.
- Recusa de pagamento sem reserva. Se a recusa chegar antes da reserva (ou o pedido nunca tiver reservado), não há o que devolver: o evento é consumido, registrado como processado e nenhum `ReservaDeEstoqueCancelada` é publicado, porque nada foi cancelado. Se a reserva chegar depois, ela não é desfeita. Aceito por ora; a mitigação seria guardar a recusa e consultá-la ao reservar.
- A coreografia não tem um lugar único para ver o estado. Quem quiser saber em que pé está a compensação de um pedido precisa juntar `PagamentoRecusado`, a tabela de reservas e `ReservaDeEstoqueCancelada`.
- O consumidor passou a ler o corpo como texto. O parsing do JSON vive em `PedidoConfirmadoLeitor`, e um corpo ilegível agora é falha permanente e vai para a DLQ, em vez de ser tratado pelo deserializador.

Fora de escopo: serviço de pagamento de verdade, interface de operação para a DLQ, alerta automático (descrito como prática de produção, mas não implementado), `ReservaDeEstoqueCancelada` com consumidor, e um outbox com relay.

## Perguntas

### 1. O que acontece se o `servico-estoque` cair no meio da compensação, depois de devolver a quantidade e antes de publicar `ReservaDeEstoqueCancelada`?

O ponto exato está em [`EstoqueListener.aoRecusarPagamento`](../../servico-estoque/src/main/java/br/pucminas/aed/estoque/controller/EstoqueListener.java#L70-L86), exatamente entre o retorno de `estoqueService.compensar(eventoId, evento)` (linha 79) e a chamada a `publicadorDeCancelamento.publicar(cancelamento)` (linha 81), antes do `ack.acknowledge()` (linha 85):

1. [`EstoqueService.compensar`](../../servico-estoque/src/main/java/br/pucminas/aed/estoque/service/EstoqueService.java#L72-L108) é `@Transactional` (linha 72). No mesmo commit ele chama `registrarEventoSeNovo(eventoId)` (linha 75), devolve o saldo de cada item com `devolverEstoque(...)` (linha 98), grava `devolvida_em` e `cancelamento_evento_id` em `reserva` via `marcarReservasDevolvidas(...)` (linha 103), e deixa `cancelamento_publicado_em` nulo;
2. fora da transação do banco, [`ReservaCanceladaPublisherService.publicar(cancelamento)`](../../servico-estoque/src/main/java/br/pucminas/aed/estoque/service/ReservaCanceladaPublisherService.java#L40-L68) envia o evento ao Kafka (chamada na linha 81 do listener) e, logo depois, `confirmarPublicacaoDoCancelamento(cancelamento)` (linha 82) preenche `cancelamento_publicado_em`;
3. `ack.acknowledge()` (linha 85) confirma o offset de `PagamentoRecusado`.

A queda entre os passos 1 e 2 é o dual write: o commit do passo 1 já devolveu o estoque no Postgres, mas o evento não saiu no Kafka. Como a linha 85 (`ack.acknowledge()`) não chegou a executar, o offset de `PagamentoRecusado` não foi confirmado e o broker reentrega a mensagem quando o serviço sobe de volta. Num consumidor ingênuo, essa reentrega bateria em [`EstoqueRepository.registrarEventoSeNovo`](../../servico-estoque/src/main/java/br/pucminas/aed/estoque/service/EstoqueRepository.java#L52-L57) (chamado na linha 75 do `EstoqueService`), receberia `false` porque o `ce_id` já está em `evento_processado`, e descartaria a mensagem em silêncio: o estoque ficaria certo, mas `ReservaDeEstoqueCancelada` nunca seria publicado, a menos que houvesse um outbox ou publicação transacional conjunta.

No código implementado, o ramo `!primeiraVez` de [`EstoqueService.compensar`](../../servico-estoque/src/main/java/br/pucminas/aed/estoque/service/EstoqueService.java#L76-L86) consulta [`EstoqueRepository.buscarCancelamentoPendente`](../../servico-estoque/src/main/java/br/pucminas/aed/estoque/service/EstoqueRepository.java#L128-L142) (linha 78): como a reserva daquele pedido tem `devolvida_em IS NOT NULL` e `cancelamento_publicado_em IS NULL`, o método reconstrói e devolve o `ReservaDeEstoqueCanceladaEvent` com o mesmo `cancelamento_evento_id` gerado antes da queda, sem executar `devolverEstoque` de novo. O listener recebe esse evento na linha 79, publica no Kafka (linha 81), grava `cancelamento_publicado_em` via [`marcarCancelamentoPublicado`](../../servico-estoque/src/main/java/br/pucminas/aed/estoque/service/EstoqueRepository.java#L144-L148) (linha 82) e confirma o offset (linha 85). Esse cenário está coberto pelo teste `CompensacaoTest.recuperaPublicacaoPendenteAposFalhaNoMeioDaCompensacao`.

Se a queda acontecer antes do commit do passo 1 terminar, o Postgres desfaz toda a transação (inclusive `evento_processado`) e a reentrega processa tudo do zero. Se a queda acontecer depois da linha 81 (evento já publicado no broker) e antes da linha 82 (`confirmarPublicacaoDoCancelamento`), a reentrega publica `ReservaDeEstoqueCancelada` uma segunda vez com o mesmo `ce_id` (`cancelamento_evento_id`), caracterizando entrega at-least-once na saída.

### 2. Que consequência você aceitou ao escolher a sua política de retentativa?

Aceitei que a política troca disponibilidade por tempo de cura: ela cobre bem a oscilação curta, e uma queda do banco acima de cerca de 22 s (3 retentativas, esperas de 2 s, 4 s e 8 s, mais 2 s de conexão em cada execução) manda eventos perfeitamente válidos para a DLQ, onde esperam por uma pessoa.

Durante essa janela, a retentativa também bloqueia a partição: todo evento atrás do que está falhando espera, e com o banco fora do ar e tráfego contínuo cada evento paga o ciclo inteiro de ~22 s antes de ir para a DLQ. O consumo cai a ~1 evento por 22 s por partição até o banco voltar, e a DLQ passa a receber tudo. Depois, o reprocessamento reinsere esses eventos fora da ordem original em que chegaram.

Escolhi assim porque a alternativa, retentativa sem limite, fazia o oposto: preferia nunca falhar a manter o consumidor andando, e deixava uma partição presa sem ninguém ser avisado. Aceito perder a recuperação automática depois de 22 s para ter um limite conhecido, um lugar onde o evento fica guardado com o motivo, e um caminho de volta que não debita duas vezes, graças à deduplicação por `ce_id`.
