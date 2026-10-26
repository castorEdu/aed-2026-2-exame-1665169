# Exame especial — como reproduzir

Esta seção cobre a **Parte A** (caminho de falha) e a **Parte B** (compensação). Os comandos assumem a raiz do
repositório, o ambiente de pé (`docker compose up -d`) e os dois serviços
empacotados (`mvn -f servico-pedidos/pom.xml package -DskipTests` e
`mvn -f servico-estoque/pom.xml package -DskipTests`), em dois terminais:

```bash
java -jar servico-pedidos/target/servico-pedidos-1.0.jar
java -jar servico-estoque/target/servico-estoque-1.0.jar
```

## O que o `servico-estoque` faz quando o evento falha

| Falha | Exemplo | Retenta? | Destino |
|---|---|---|---|
| Permanente | SKU que não existe em `estoque` (`SkuInexistenteException`), quantidade ≤ 0 (`QuantidadeInvalidaException`), JSON ilegível ou sem `ce_id` (`EventoMalformadoException`) | não | `loja.pedido.confirmado.v1.dlq`, na primeira falha |
| Transitória | Postgres fora do ar (`docker compose stop postgres`) | sim, até 3 retentativas | volta ao fluxo se a causa passar; vai à DLQ se esgotar |
| Inesperada | qualquer outra exceção | sim, até 3 retentativas | DLQ se esgotar |

Política de retentativa, configurada em `servico-estoque/src/main/resources/application.yml`
(`demo.retentativa.*`) e montada em `ResilienciaConfig`:

| Tipo de falha | BackOff | Esperas | Total |
|---|---|---|---|
| Transitória (banco ou conexão) | `ExponentialBackOff` (2 s, ×2, teto de 10 s) | 2 s, 4 s, 8 s | até 14 s de espera |
| Inesperada | `FixedBackOff` (1 s) | 1 s, 1 s, 1 s | 3 s de espera |
| Permanente | nenhum | — | vai direto para a DLQ |

O limite é de **3 retentativas depois da entrega original**, ou seja, no máximo 4
execuções do mesmo evento. A espera cresce para a falha transitória porque ela
costuma ser sobrecarga ou indisponibilidade, que pede tempo para passar. A falha
inesperada tem espera fixa e curta: se for um defeito de código, esperar mais não
ajuda, e a DLQ preserva o evento sem bloquear a partição.

### O que chega na DLQ

O evento é publicado com a **mesma chave, o mesmo corpo e todos os cabeçalhos
`ce_*` originais** (`ce_id` inclusive), mais o motivo da falha:

| Cabeçalho | Conteúdo |
|---|---|
| `dlq_excecao` | classe da exceção, por exemplo `br.pucminas.aed.estoque.domain.SkuInexistenteException` |
| `dlq_motivo` | mensagem da exceção |
| `dlq_falhou_em` | instante da falha, ISO-8601 |
| `kafka_dlt-original-topic`, `-partition`, `-offset`, `-consumer-group` | de onde o evento veio (cabeçalhos do Spring Kafka) |
| `kafka_dlt-exception-cause-fqcn`, `-message`, `-stacktrace` | a exceção completa (cabeçalhos do Spring Kafka) |

## Cenário 1 — falha permanente: SKU inexistente

```powershell
curl.exe -s -w "`nHTTP %{http_code}`n" -X POST localhost:8080/pedidos/confirmados `
  -H "Content-Type: application/json" `
  -d '{\"eventoId\":\"aaaaaaaa-0000-0000-0000-000000000001\",\"ocorridoEm\":\"2026-10-26T12:00:00Z\",\"pedidoId\":\"9001\",\"clienteId\":\"c-1\",\"valorTotal\":10.0,\"moeda\":\"BRL\",\"itens\":[{\"sku\":\"TEC-404\",\"quantidade\":1,\"preco\":10.0}]}'
```

Saída esperada:

- `HTTP 202`;
- no terminal do `servico-estoque`, **uma única** falha, sem retentativas:

  ```text
  WARN  RegistroDeFalhas : falha na execucao 1  chave=9001  offset=1  causa=SkuInexistenteException: SKU inexistente no estoque: TEC-404
  ERROR RegistroDeFalhas : evento enviado para loja.pedido.confirmado.v1.dlq  chave=9001  offset=1  causa=SkuInexistenteException: SKU inexistente no estoque: TEC-404
  ```
- o estoque de `TEC-002` continua em 100 e `evento_processado` continua vazio, porque a transação inteira é desfeita:

```powershell
docker compose exec postgres psql -U aed -d aed -c "select * from estoque; select count(*) from evento_processado;"
```

- no Kafka UI (<http://localhost:8081>), tópico `loja.pedido.confirmado.v1.dlq`, uma mensagem com `ce_id = aaaaaaaa-0000-0000-0000-000000000001`, `dlq_excecao = ...SkuInexistenteException` e `dlq_motivo = SKU inexistente no estoque: TEC-404`.

Para ler a DLQ pelo console:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9094 --topic loja.pedido.confirmado.v1.dlq --from-beginning --timeout-ms 5000 --property print.headers=true --property print.key=true
```

## Cenário 2 — falha transitória: Postgres fora do ar

```powershell
docker compose stop postgres
curl.exe -s -w "HTTP %{http_code}`n" -X POST localhost:8080/pedidos/confirmados `
  -H "Content-Type: application/json" -d "@pedidos/pedido-4712.json"
```

Saída esperada, no terminal do `servico-estoque`: uma linha por execução que falha,
espaçadas por 2 s, 4 s e 8 s de espera (mais 2 s de tentativa de conexão em cada
execução):

```text
WARN  RegistroDeFalhas : falha na execucao 1  chave=4712  offset=0  causa=CannotCreateTransactionException: Could not open JDBC Connection for transaction
WARN  RegistroDeFalhas : falha na execucao 2  chave=4712  offset=0  causa=CannotCreateTransactionException: Could not open JDBC Connection for transaction
```

**Religue o Postgres antes de esgotar a janela (cerca de 20 s):**

```powershell
docker compose start postgres
```

Na próxima retentativa o evento é processado:

```text
INFO  EstoqueService : reserva aplicada  evento=22222222-2222-2222-2222-222222222222  pedido=4712
```

O estoque de `TEC-002` cai em 2 e a DLQ continua sem o evento. Se o Postgres demorar mais que a
janela, o evento vai para a DLQ com `dlq_excecao` apontando para a falha de
conexão, e pode ser reprocessado como descrito abaixo.

## Reprocessar a DLQ

Corrija a causa (no cenário 1, cadastre o SKU) e rode o `servico-estoque` em modo
de reprocessamento:

```powershell
docker compose exec postgres psql -U aed -d aed -c "INSERT INTO estoque (sku, quantidade) VALUES ('TEC-404', 10);"
java -jar servico-estoque/target/servico-estoque-1.0.jar --demo.reprocessar-dlq=true
```

Nesse modo o aplicativo **não** consome o tópico normal (pode rodar com outro `servico-estoque` já no ar, sem rebalancear as partições). Ele lê a
`loja.pedido.confirmado.v1.dlq`, republica cada evento em
`loja.pedido.confirmado.v1` com a mesma chave, o mesmo corpo e os cabeçalhos
`ce_*` originais (`ce_id` inclusive), confirma o que leu e encerra com a mensagem
`reprocessamento concluido: N evento(s) republicado(s)`. Os cabeçalhos
`dlq_*` e `kafka_dlt-*` não voltam ao fluxo.

Depois, suba o `servico-estoque` normalmente: o evento é consumido como qualquer
outro, e a deduplicação por `ce_id` garante que um evento já processado não debita
o estoque de novo.

- O reprocessamento lembra até onde leu (grupo `estoque-dlq-reprocessador`): rodar de novo não republica o que já foi republicado. Para reler tudo, use outro grupo: `--demo.dlq.grupo-reprocessador=outro-nome`.
- Se o evento falhar de novo, ele volta para a DLQ.

## Cenário 3 — compensação: pagamento recusado devolve a reserva

1. Publique um pedido confirmado (`pedidoId = 4711`, reserva 1 unidade de `TEC-002`):

```powershell
curl.exe -s -w "`nHTTP %{http_code}`n" -X POST localhost:8080/pedidos/confirmados `
  -H "Content-Type: application/json" -d "@pedidos/pedido-4711.json"
```

2. Confira o estoque (`TEC-002` cai para 99) e a linha criada em `reserva` (`devolvida_em` nulo):

```powershell
docker compose exec postgres psql -U aed -d aed -c "select * from estoque; select pedido_id, sku, quantidade, reservada_em, devolvida_em, cancelamento_evento_id from reserva;"
```

3. Publique a recusa do pagamento (`PagamentoRecusado` em `loja.pagamento.recusado.v1`):

```powershell
curl.exe -s -w "`nHTTP %{http_code}`n" -X POST localhost:8080/pedidos/pagamentos-recusados `
  -H "Content-Type: application/json" -d "@pedidos/pagamento-recusado-4711.json"
```

4. Saída esperada:

- `HTTP 202`;
- no terminal do `servico-estoque`:

  ```text
  INFO  EstoqueService : reserva devolvida  evento=33333333-3333-3333-3333-333333333333  pedido=4711  cancelamento=...
  INFO  ReservaCanceladaPublisherService : publicado  evento=...  pedido=4711  particao=...  offset=...
  ```

- no banco, a quantidade de `TEC-002` volta a 100 e a reserva permanece registrada com `devolvida_em` e `cancelamento_evento_id` preenchidos (sem `DELETE`):

```powershell
docker compose exec postgres psql -U aed -d aed -c "select * from estoque; select pedido_id, sku, quantidade, reservada_em, devolvida_em, cancelamento_evento_id, cancelamento_publicado_em from reserva;"
```

- no tópico `loja.estoque.reservacancelada.v1` (Kafka UI em <http://localhost:8081> ou console), aparece o evento `ReservaDeEstoqueCancelada` com cabeçalhos CloudEvents (`ce_specversion=1.0`, `ce_id`, `ce_source=/loja/servico-estoque`, `ce_type=loja.estoque.reservacancelada.v1`, `ce_time` em ISO-8601):

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9094 --topic loja.estoque.reservacancelada.v1 --from-beginning --timeout-ms 5000 --property print.headers=true --property print.key=true
```

- se o comando do passo 3 for executado uma segunda vez (reentrega do mesmo `PagamentoRecusado`), o estoque permanece em 100 e nenhum novo evento é publicado.

## Testes automatizados

```bash
mvn -f servico-estoque/pom.xml test
```

Kafka embutido e H2, sem Docker. Os intervalos de espera são reduzidos nos testes.

| Teste | O que cobre |
|---|---|
| `CaminhoDeFalhaTest` 1 | Cenário 1: SKU inexistente vai à DLQ sem retentativa, com `ce_id`, `dlq_excecao`, `dlq_motivo` e `dlq_falhou_em` |
| `CaminhoDeFalhaTest` 2 | quantidade ≤ 0 vai à DLQ sem retentativa |
| `CaminhoDeFalhaTest` 3 | Cenário 2: duas falhas transitórias, sucesso na terceira execução, DLQ vazia |
| `CaminhoDeFalhaTest` 4 | falha transitória que não passa: 1 + 3 execuções e depois DLQ |
| `CaminhoDeFalhaTest` 5 | JSON inválido vai à DLQ com o corpo original |
| `ReprocessamentoDlqTest` 6 | evento da DLQ volta ao tópico com o mesmo `ce_id` e é processado depois da correção |
| `ReprocessamentoDlqTest` 7 | segunda execução do reprocessamento não republica nada |
| `CompensacaoTest` 1 | Cenário 3: `PagamentoRecusado` devolve a quantidade reservada e publica `ReservaDeEstoqueCancelada` com cabeçalhos CloudEvents e ISO-8601 |
| `CompensacaoTest` 2 | mesma recusa entregue duas vezes (e segunda recusa para o mesmo pedido) devolve o estoque uma só vez e publica um só evento |
| `CompensacaoTest` 3 | recusa de pedido sem reserva não altera o estoque e não publica cancelamento |
| `CompensacaoTest` 4 | queda após devolver o estoque e antes de publicar: a reentrega publica `ReservaDeEstoqueCancelada` pendente sem devolver o estoque duas vezes |

---
# Aula 01 — Demonstração: o consumidor idempotente

**AED — Arquitetura Reativa e Event-Driven · PUC Minas / IEC**

Duas aplicações Spring Boot que conversam **apenas por eventos**. Você vai
entregar o mesmo evento três vezes e ver o estoque ser debitado **uma só vez**.
Depois vai desligar a proteção e ver a cobrança dupla acontecer.

> **Isto não é a entrega da aula 01.** A atividade avaliada é o diário de
> eventos, no enunciado do Canvas. Rodar esta demonstração serve a dois
> propósitos: entender na prática o que foi discutido em aula e, principalmente,
> **provar que o seu ambiente está pronto para a aula 02**, onde a prática
> começa sem tempo para instalação.
>
> Se algo aqui não funcionar, **abra no fórum da aula 01 durante a semana** — não
> no dia da aula 02.

---

## O que você vai observar

Três perguntas que a demonstração responde:

1. O mesmo evento chega três vezes ao tópico. Por que o estoque cai só uma vez?
2. Se eu desligar a deduplicação, o que exatamente quebra — a entrega ou o
   efeito?
3. Com o consumidor **desligado**, a API ainda aceita pedidos? O que acontece
   com eles?

---

## Pré-requisitos

| O que | Como verificar | Esperado |
|---|---|---|
| **JDK 21** | `java -version` | `openjdk version "21..."` |
| **Maven 3.9+** | `mvn -v` | versão 3.9 ou maior, com `Java version: 21` |
| **Docker** | `docker compose version` | qualquer v2.x |
| **Docker rodando** | `docker ps` | uma tabela, mesmo vazia |

**Se `java -version` disser 17 ou 11**, o projeto não vai iniciar — os `.jar`
são compilados para a versão 21 e o erro é
`UnsupportedClassVersionError: class file version 65.0`. Ajuste o `JAVA_HOME` e
o `PATH` para um JDK 21 antes de continuar.

**Se `docker ps` reclamar que não consegue conectar**, o Docker Desktop não está
em execução. Abra-o e espere ficar verde.

---

## Windows: `curl` não é o curl

No PowerShell, **`curl` é apelido de `Invoke-WebRequest`**, que não entende
`-X`, `-d` nem `@arquivo`. O comando parece certo e falha.

| Shell | O que digitar |
|---|---|
| PowerShell | **`curl.exe`** — com o `.exe` |
| Git Bash, WSL, Linux, macOS | `curl` |

Confira com `Get-Command curl`: se responder `Alias`, use `curl.exe`.

Neste README os comandos aparecem com `curl.exe`. Em Linux e macOS, tire o
`.exe` e troque a crase de quebra de linha (`` ` ``) por barra invertida (`\`).

---

## Passo 0 — descompactar

O código vem num **ZIP no Canvas**, no módulo da aula 01. Baixe e descompacte
onde preferir.

**Evite descompactar dentro de uma pasta sincronizada** — OneDrive, Google
Drive, Dropbox. A sincronização compete com o Maven pela pasta `target/` e
produz erros de arquivo em uso que parecem bug do projeto. Uma pasta simples,
como `C:\dev\aed` ou `~/dev/aed`, evita o problema.

Depois de descompactar, entre na pasta:

```bash
cd demo-kafka-idempotencia
```

**Todos os comandos deste README rodam a partir desta pasta**, a raiz do
projeto — nunca de dentro de `servico-pedidos/`. Os caminhos dos `.jar` e dos
arquivos de pedido são relativos a ela. Três erros diferentes vêm de estar na
pasta errada, e nenhum deles diz "pasta errada":

| Comando | Mensagem enganosa |
|---|---|
| `java -jar servico-pedidos/target/...` | `Unable to access jarfile` |
| `curl.exe ... -d "@pedidos/pedido-4711.json"` | falha ao ler o arquivo |
| `docker compose down` | `no configuration file provided: not found` |

---

## Passo 1 — subir a infraestrutura

```bash
docker compose up -d
docker compose ps
```

Sobem três contêineres: **Kafka**, **Postgres** e a **Kafka UI**. O comando
demora alguns segundos e só retorna quando o Kafka está pronto — isso é
proposital.

Esperado:

```
NAME           STATUS
aed-kafka      Up (healthy)
aed-kafka-ui   Up
aed-postgres   Up (healthy)
```

> As portas são **19092** (Kafka) e **15432** (Postgres), e não as padrão. Muita
> gente já tem Kafka ou Postgres na 9092 e na 5432; quando isso acontece, a
> aplicação conecta no serviço errado e o erro que aparece é de **autenticação**,
> que manda você procurar no lugar errado.

---

## Passo 2 — compilar

```bash
mvn -f servico-pedidos/pom.xml package -DskipTests
mvn -f servico-estoque/pom.xml package -DskipTests
```

A primeira execução baixa dependências e demora. **Faça isso antes da aula 02**,
com internet boa.

Confira que os dois `.jar` existem:

```bash
ls servico-*/target/*.jar          # Linux, macOS, Git Bash
dir servico-*\target\*.jar         # PowerShell
```

Sem este passo, o próximo comando responde `Unable to access jarfile`: a pasta
`target/` só existe depois do `package`.

---

## Passo 3 — subir os dois serviços

Em **dois terminais separados**, para poder derrubar um sem o outro.

Terminal 1 — **suba este primeiro**, é ele que cria o tópico:

```bash
java -jar servico-pedidos/target/servico-pedidos-1.0.jar
```

Terminal 2:

```bash
java -jar servico-estoque/target/servico-estoque-1.0.jar
```

Deixe **os dois visíveis**: os logs são metade da demonstração.

Esperado, no fim de cada um: `Started PedidoApplication` e
`Started EstoqueApplication`.

> Se você subir o consumidor primeiro, ele reclama de `UNKNOWN_TOPIC_OR_PARTITION`
> até o tópico existir. Não é erro fatal, mas atrasa.

### Como observar o estado

O `servico-estoque` **não expõe HTTP de propósito** — um consumidor não precisa
de porta. O estado se observa no banco:

```bash
docker compose exec postgres psql -U aed -d aed -c "select * from estoque; select count(*) from evento_processado;"
```

Esperado agora: `TEC-002 | 100` e `count = 0`.

---

## Passo 4 — publicar um pedido

O evento **não é inventado pela aplicação**: ele vai no corpo da requisição e é
desserializado direto na classe de domínio. Não há fábrica nem endpoint que gere
pedido. Os payloads prontos estão em `pedidos/`.

Abra `pedidos/pedido-4711.json` antes de enviar. Repare no `eventoId` **fixo** —
é ele que faz a repetição ser o *mesmo* evento:

```json
{ "eventoId":   "11111111-1111-1111-1111-111111111111",
  "ocorridoEm": "2026-08-03T22:14:07.512Z",
  "pedidoId":   "4711",
  "clienteId":  "c-9931",
  "valorTotal": 289.90,
  "moeda":      "BRL",
  "itens": [ { "sku": "TEC-002", "quantidade": 1, "preco": 289.90 } ] }
```

```powershell
curl.exe -s -w "`nHTTP %{http_code}`n" -X POST localhost:8080/pedidos/confirmados `
  -H "Content-Type: application/json" -d "@pedidos/pedido-4711.json"
```

Esperado: **HTTP 202**.

> **Pare no 202, não é 200.** A API não confirmou a reserva — confirmou que
> *aceitou* o pedido. No instante da resposta o estoque ainda não mudou, e o
> serviço que muda pode nem estar rodando. É o passo 7 que torna isso visível.

Nos logs:

```
[pedidos] publicado  evento=11111111-...  pedido=4711  particao=1  offset=0
[estoque] reserva aplicada  evento=11111111-...  pedido=4711
```

Confira o banco: **estoque 100 → 99**, `evento_processado` com 1 linha.

---

## Passo 5 — repetir o MESMO comando (o ponto da aula)

Seta para cima e Enter. **Duas vezes.** É literalmente o mesmo evento, porque o
`eventoId` está no arquivo.

```
[pedidos] publicado  evento=11111111-...  particao=1  offset=1
[estoque] evento 11111111-... JA PROCESSADO, descartando em silencio
[pedidos] publicado  evento=11111111-...  particao=1  offset=2
[estoque] evento 11111111-... JA PROCESSADO, descartando em silencio
```

O estoque **permanece 99**.

> **Pense antes de olhar o banco:** o evento chegou ao consumidor, e o `offset`
> avançou de 0 para 2 — são três registros distintos no tópico. Por que o
> estoque não caiu três vezes?
>
> Porque **chegar ao tópico e ter efeito são coisas diferentes**. O broker
> entregou as três vezes, corretamente. Quem decidiu não repetir o efeito foi o
> consumidor, consultando a tabela `evento_processado` pela chave do evento.

---

## Passo 6 — desligar a deduplicação

Não há endpoint para isso. Derrube o consumidor e suba com uma propriedade:

```bash
# Ctrl+C no terminal 2, depois:
java -jar servico-estoque/target/servico-estoque-1.0.jar --demo.deduplicacao-ativa=false
```

Republique o mesmo pedido do passo 4:

```
[estoque] deduplicacao DESLIGADA - processando 11111111-... sem verificar
[estoque] reserva aplicada  evento=11111111-...  pedido=4711
```

Estoque: **99 → 98**. É a cobrança dupla.

> Repare no que **não** mudou: o Kafka, o produtor, o contrato do evento, a
> entrega. Tudo continuou correto. O que quebrou foi o **efeito de negócio** —
> e a responsabilidade era do consumidor.

Suba de novo **sem** a propriedade e republique: o estoque não se move mais.

**Use `Ctrl+C`, nunca matar o processo à força.** Matando à força, o coordenador
do grupo não sabe que o consumidor saiu e espera o `session.timeout` expirar:
são uns 30 segundos de tela parada.

---

## Passo 7 — o desacoplamento temporal

O momento mais forte da demonstração.

```bash
# Ctrl+C no terminal 2 — o consumidor SAI DO AR
```

Com o consumidor morto, publique **quatro pedidos diferentes**. Use
`pedidos/pedido-4712.json` e crie outros copiando um arquivo e trocando
**`eventoId` e `pedidoId`** — trocar só o `pedidoId` não adianta, porque a
deduplicação olha o `eventoId`.

Todos respondem **202**. Olhe o banco: **o estoque não mudou.**

Agora suba o consumidor de novo. Ele processa tudo o que perdeu, de uma vez.

> A API aceitou pedidos com o serviço de estoque **morto**, e nada se perdeu.
> Isso é desacoplamento temporal demonstrado, não afirmado. Numa chamada HTTP
> síncrona entre os dois serviços, esses quatro pedidos teriam falhado.

---

## Kafka UI — <http://localhost:8081>

Sobe junto com o `docker compose up -d`. Sem login. O cluster chama-se `aed`.

| Onde | O que ver |
|---|---|
| **Topics** | `loja.pedido.confirmado.v1`, com 3 partições |
| Topics → Messages | cada evento: chave, corpo, partição, offset |
| Messages → uma mensagem → **Headers** | `ce_specversion`, `ce_id`, `ce_source`, `ce_type`, `ce_time` |
| **Consumers** | o grupo `estoque` e o **lag** por partição |

Vale abrir os **cabeçalhos** de uma mensagem: é o envelope CloudEvents em modo
binário, com os atributos fora do corpo. O consumidor consegue deduplicar pelo
`ce_id` sem sequer desserializar a carga.

No passo 7, acompanhe o **lag** subindo com o consumidor desligado, e caindo a
zero quando ele volta.

---

## Recomeçar do zero

Voltar o estoque a 100 e limpar a memória de deduplicação:

```bash
docker compose exec postgres psql -U aed -d aed -c "UPDATE estoque SET quantidade=100; DELETE FROM evento_processado;"
```

Isso limpa o **banco**, não o tópico: os eventos publicados continuam no Kafka.

Para apagar tudo, inclusive o tópico e os volumes:

```bash
docker compose down -v
```

---

## Rodar os testes (sem Docker)

```bash
mvn -f servico-estoque/pom.xml test
```

Quatro testes com Kafka embutido e H2 — **sem Docker e sem o `servico-pedidos`**.
Poder testar o consumidor sem subir o produtor é justamente o que a separação em
duas aplicações permite: os dois lados só se conhecem pelo tópico.

```
1 - evento novo reserva estoque uma vez
2 - o MESMO evento entregue tres vezes reserva uma so vez
3 - sem deduplicacao, a mesma entrega debita o estoque de novo
4 - campos que o consumidor nao declara sao ignorados

Tests run: 4, Failures: 0, Errors: 0
```

Se este comando passar, seu ambiente Java e Maven está correto — falta só o
Docker.

---

## Problemas comuns

| Sintoma | Causa provável | O que fazer |
|---|---|---|
| `UnsupportedClassVersionError ... 65.0` | Java menor que 21 | conferir `java -version`; ajustar `JAVA_HOME` e `PATH` |
| `Unable to access jarfile` | faltou o `mvn package`, ou você está na pasta errada | rodar o passo 2, a partir da raiz do projeto |
| `no configuration file provided: not found` | `docker compose` fora da pasta do projeto | `cd` para a raiz, ou usar `-f caminho/docker-compose.yml` |
| `curl` reclama de parâmetro no PowerShell | apelido de `Invoke-WebRequest` | usar `curl.exe` |
| `password authentication failed` | outro Postgres na sua máquina atendendo na porta | conferir se o `docker compose ps` mostra `aed-postgres`; portas aqui são 15432 e 19092 |
| `Connection refused` na porta 8080 | `servico-pedidos` não subiu | ver o terminal 1 |
| Consumidor não processa nada | subiu antes do tópico existir | esperar alguns segundos, ou reiniciá-lo |
| Estoque não volta a 100 ao reiniciar | é intencional | usar o comando de recomeçar do zero |
| Eventos somem, metade não é processada | há **mais de um** `servico-estoque` rodando | encerrar os extras: as partições se dividem entre eles |

Se nada disso resolver, poste no **fórum da aula 01** com: o comando que rodou,
a mensagem completa, e a saída de `java -version` e `docker compose ps`.

---

## Onde olhar no código

O projeto é deliberadamente **verboso e orientado a objetos**: sem lambdas ou
streams onde uma classe nomeada resolve. A intenção é que haja o que abrir no
depurador e o que apontar em aula.

### `servico-pedidos` — API REST e produtor (porta 8080, sem banco)

| Arquivo | O que ensina |
|---|---|
| `domain/PedidoConfirmadoEvent.java` | evento imutável explícito: `private final`, cópia defensiva, sem setter |
| `domain/ItemDoPedidoVO.java` | value object: igualdade por valor, sem identidade |
| `controller/PedidoController.java` | `@RequestBody` e **202 Accepted** |
| `service/PedidoService.java` | chave de partição e o envelope CloudEvents nos cabeçalhos |
| `service/PedidoCallbackService.java` | o retorno de `send()` tem dono; ignorá-lo esconde a falha |
| `PedidoConfig.java` | criação do tópico com 3 partições |

### `servico-estoque` — consumidor e banco (sem servidor web)

| Arquivo | O que ensina |
|---|---|
| `domain/PedidoConfirmadoEvent.java` | **declara menos campos que o produtor** — consumidor tolerante |
| `controller/EstoqueListener.java` | o `ack` vem **depois** do commit: é o que caracteriza at-least-once |
| `service/EstoqueService.java` | `@Transactional`: deduplicação e efeito de negócio no **mesmo commit** |
| `service/EstoqueRepository.java` | as duas tabelas: memória do que já foi visto, e o efeito |
| `resources/schema.sql` | a tabela `evento_processado` e a nota sobre retenção da chave |

> Os dois projetos são Maven **independentes**: sem POM pai, sem módulo de
> contrato compartilhado. Cada lado declara a **própria** classe do evento — e o
> consumidor declara menos campos que o produtor, de propósito. O contrato entre
> eles é o JSON que trafega no tópico, e mais nada. Compartilhar um JAR recriaria
> o acoplamento de compilação que os eventos deveriam eliminar.
