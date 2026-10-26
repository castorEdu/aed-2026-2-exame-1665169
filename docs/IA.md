# Registro do uso de IA — Exame especial

A IA foi utilizada para acelerar a escrita do código, dos testes automatizados e da documentação (`README.md` e `ADR-006-resiliencia.md`) a partir da minha orientação direta sobre as regras de negócio, o padrão arquitetural da disciplina e os requisitos do exame. As decisões de desenho, classificação de falhas, semântica dos eventos e garantias transacionais foram definidas e revisadas por mim antes de irem para o repositório.

---

## Interação 1 — Caminho de falha, `DefaultErrorHandler` e preservação do payload na DLQ

### O que foi pedido
Configurar o `DefaultErrorHandler` com `DeadLetterPublishingRecoverer` no `servico-estoque`, distinguindo falhas permanentes (`SkuInexistenteException` e `QuantidadeInvalidaException`) de falhas transitórias de conexão com o Postgres, e publicar o evento na DLQ (`loja.pedido.confirmado.v1.dlq`) mantendo os cabeçalhos CloudEvents (`ce_*`) e adicionando o motivo da falha.

### O que a ferramenta sugeriu
1. Criar a hierarquia `FalhaPermanenteException` e registrá-la em `addNotRetryableExceptions`, verificando o retorno de `jdbc.update` em `EstoqueRepository.reservar()` para lançar `SkuInexistenteException` quando zero linhas forem afetadas.
2. Usar `ExponentialBackOff` sem configurar `maxAttempts` (ou deixando o limite padrão aberto no backoff exponencial) e manter o `JsonDeserializer<PedidoConfirmadoEvent>` no consumidor para alimentar o `DeadLetterPublishingRecoverer`.
3. Adicionar cabeçalhos próprios (`dlq_excecao`, `dlq_motivo`, `dlq_falhou_em`) via `setHeadersFunction` no `DeadLetterPublishingRecoverer`.

### O que foi aceito
- A hierarquia `FalhaPermanenteException` (`SkuInexistenteException`, `QuantidadeInvalidaException`, `EventoMalformadoException`) lançada dentro do método `@Transactional`, garantindo o rollback do `INSERT` em `evento_processado` para permitir reprocessamento futuro.
- Os cabeçalhos explícitos `dlq_excecao`, `dlq_motivo` e `dlq_falhou_em` (em ISO-8601) somados aos cabeçalhos `ce_*` originais.
- A combinação de `ExponentialBackOff` (2 s, 4 s, 8 s) para falhas transitórias de banco e `FixedBackOff` (1 s) para falhas inesperadas via `setBackOffFunction`.

### O que foi recusado e por quê
- **Recusa 1 — Backoff sem limite explícito de tentativas**: um `ExponentialBackOff` sem `maxAttempts` finito faz o consumidor tentar para sempre quando o banco cai por mais tempo ou quando uma falha permanente escapa da lista de exceções, travando a partição inteira (*head-of-line blocking*) e impedindo o envio para a DLQ. Exigi o limite estrito de 3 retentativas após a entrega original (máximo de 4 execuções), documentado no `ADR-006`.
- **Recusa 2 — Publicar na DLQ o objeto já desserializado pelo `JsonDeserializer`**: como `PedidoConfirmadoEvent` no `servico-estoque` é um consumidor tolerante e declara apenas quatro campos (`eventoId`, `ocorridoEm`, `pedidoId`, `itens`), re-serializar essa instância para a DLQ apagaria `clienteId`, `valorTotal`, `moeda` e `preco` do JSON original, além de impedir que um JSON malformado chegasse à DLQ com o corpo que falhou. Orientei trocar o consumidor para receber `String` e fazer a leitura em `PedidoConfirmadoLeitor`, preservando o payload original byte a byte na DLQ e no reprocessamento.

---

## Interação 2 — Registro das reservas e compensação após `PagamentoRecusado`

### O que foi pedido
Implementar a tabela de reservas e o fluxo de compensação quando o `servico-estoque` consome `PagamentoRecusado` do tópico `loja.pagamento.recusado.v1`, devolvendo a quantidade ao estoque e publicando o evento de compensação em `loja.estoque.reservacancelada.v1`.

### O que a ferramenta sugeriu
1. Criar a tabela `reserva`, somar a quantidade de volta em `estoque` e executar `DELETE FROM reserva WHERE pedido_id = ?` para limpar a reserva desfeita.
2. Nomear o evento ou método de saída como `DesfazerReservaEvent` / `ReservaDeletadaEvent` e expor um endpoint HTTP no `servico-estoque` caso o `servico-pedidos` quisesse acionar o estorno diretamente.
3. Publicar o evento no tópico com `KafkaTemplate` usando o `ObjectMapper` padrão do Spring Kafka.

### O que foi aceito
- A criação da tabela `reserva` populada na mesma transação (`@Transactional`) de `EstoqueService.processar`.
- O endpoint `POST /pedidos/pagamentos-recusados` (respondendo `202 Accepted`) no `servico-pedidos` apenas para simular a publicação do fato `PagamentoRecusado` no tópico `loja.pagamento.recusado.v1` com cabeçalhos CloudEvents.
- O consumo coreografado no `servico-estoque` devolvendo exatamente os itens reservados daquele pedido.

### O que foi recusado e por quê
- **Recusa 1 — Usar `DELETE FROM reserva` para desfazer a reserva**: apagar a linha destrói o histórico de que a reserva existiu e impede distinguir "pedido que nunca reservou" de "pedido cuja reserva já foi devolvida" caso chegue uma segunda recusa com outro `ce_id`. Determinou-se manter o registro imutável do fato e preencher `devolvida_em` e `cancelamento_evento_id`.
- **Recusa 2 — Nomes no imperativo ou de banco (`DesfazerReserva` / `ReservaDeletada`) e chamada HTTP entre serviços**: `DesfazerReserva` é comando e `ReservaDeletada` é vocabulário de CRUD; além disso, chamada HTTP síncrona acopla os serviços temporalmente. Manteve-se `ReservaDeEstoqueCanceladaEvent` (particípio, fato consumado do domínio) trafegando exclusivamente por evento em `loja.estoque.reservacancelada.v1`, mantendo `spring.main.web-application-type: none` no `servico-estoque`.
- **Recusa 3 — Serializador padrão com datas em epoch**: o `ObjectMapper` padrão serializa `Instant` como número decimal de segundos, violando o contrato JSON no fio. Foi exigido `disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)` para garantir datas em ISO-8601 tanto no corpo quanto no cabeçalho `ce_time`.

---

## Interação 3 — Idempotência na compensação e falha entre o commit e a publicação (*dual write*)

### O que foi pedido
Garantir que a compensação seja idempotente e analisar o que acontece no código se o `servico-estoque` cair depois de devolver a quantidade no Postgres e antes de publicar `ReservaDeEstoqueCancelada` no Kafka (Pergunta 1 do `ADR-006`).

### O que a ferramenta sugeriu
1. Colocar `publicadorDeCancelamento.publicar(cancelamento)` dentro do método `@Transactional compensar(...)` e confiar apenas no `registrarEventoSeNovo(eventoId)` na entrada: se `registrarEventoSeNovo` retornar `false`, descartar imediatamente e retornar.

### O que foi aceito
- Manter a gravação de `evento_processado`, a devolução do saldo em `estoque` e a marcação da reserva na mesma transação local do banco (`@Transactional` em `EstoqueService.compensar`), com o `ack.acknowledge()` no listener somente após a conclusão de todo o fluxo.

### O que foi recusado e por quê
- **Recusa — Descartar cegamente na reentrega quando `registrarEventoSeNovo` retorna `false`**: como o Kafka não participa da transação JDBC do Postgres, se o processo cair após o commit de `compensar()` e antes de `publicadorDeCancelamento.publicar()` (ou se o broker falhar nesse instante), o `ack` não terá ocorrido e `PagamentoRecusado` será reentregue. Se o código apenas fizesse `if (!primeiraVez) return;`, a reentrega veria o `ce_id` em `evento_processado` e descartaria o evento em silêncio — o estoque estaria devolvido, mas `ReservaDeEstoqueCancelada` jamais seria publicado. Orientei gravar `cancelamento_evento_id` no mesmo commit da devolução e deixar `cancelamento_publicado_em` nulo até a confirmação do broker; na reentrega (`!primeiraVez`), o serviço verifica `buscarCancelamentoPendente(pedidoId)`, publica o evento pendente com o mesmo `ce_id` sem devolver o estoque duas vezes e só então marca `cancelamento_publicado_em`.
