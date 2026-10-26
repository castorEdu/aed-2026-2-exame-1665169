package br.pucminas.aed.estoque;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;

import java.time.Duration;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource;

import br.pucminas.aed.estoque.service.EstoqueRepository;
import br.pucminas.aed.estoque.service.ModoDaDemo;
import br.pucminas.aed.estoque.service.ReservaCanceladaPublisherService;

@SpringBootTest
@EmbeddedKafka(partitions = 3, topics = {
        "loja.pedido.confirmado.v1",
        "loja.pedido.confirmado.v1.dlq",
        "loja.pagamento.recusado.v1",
        "loja.estoque.reservacancelada.v1"
})
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.datasource.url=jdbc:h2:mem:aed_compensacao;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "demo.retentativa.espera-inicial-ms=100",
        "demo.retentativa.multiplicador=2.0",
        "demo.retentativa.espera-maxima-ms=400",
        "demo.retentativa.espera-fixa-ms=100",
        "logging.level.br.pucminas.aed=INFO"
})
class CompensacaoTest {

    private static final String SKU = "TEC-002";
    private static final int ESTOQUE_INICIAL = 100;
    private static final Duration PRAZO = Duration.ofSeconds(20);
    private static final Duration JANELA_DE_OBSERVACAO = Duration.ofSeconds(2);

    @Autowired
    private EstoqueRepository repositorio;
    @Autowired
    private ModoDaDemo modo;
    @SpyBean
    private ReservaCanceladaPublisherService publicadorDeCancelamento;

    @Value("${spring.embedded.kafka.brokers}")
    private String servidores;
    @Value("${demo.topico}")
    private String topicoPedidos;
    @Value("${demo.topico-pagamento-recusado}")
    private String topicoPagamentoRecusado;
    @Value("${demo.topico-reserva-cancelada}")
    private String topicoReservaCancelada;

    private TestPublisher publicador;
    private ObservadorDeTopico cancelamentos;

    @BeforeEach
    void prepararEstado() {
        repositorio.redefinirEstoque(SKU, ESTOQUE_INICIAL);
        repositorio.limparReservas();
        repositorio.limparEventosProcessados();
        modo.setDeduplicacaoAtiva(true);
        publicador = new TestPublisher(servidores, topicoPedidos);
        cancelamentos = new ObservadorDeTopico(servidores, topicoReservaCancelada);
    }

    @Test
    @DisplayName("1 - PagamentoRecusado devolve a quantidade reservada e publica ReservaDeEstoqueCancelada com CloudEvents")
    void pagamentoRecusadoDevolveEstoqueEPublicaCancelamento() {
        String pedidoId = "7001";
        publicador.publicar(UUID.randomUUID().toString(), pedidoId, SKU, 4);
        aguardarEstoque(96);
        assertThat(repositorio.contarReservasAtivas(pedidoId)).isEqualTo(1L);

        publicador.publicarRecusa(topicoPagamentoRecusado, UUID.randomUUID().toString(), pedidoId, "cartao recusado");

        aguardarEstoque(100);
        assertThat(repositorio.contarReservasAtivas(pedidoId)).isZero();
        assertThat(repositorio.contarReservasDevolvidas(pedidoId)).isEqualTo(1L);

        ConsumerRecord<String, String> eventoCancelamento = cancelamentos.aguardarComChave(pedidoId);
        assertThat(ObservadorDeTopico.cabecalho(eventoCancelamento, "ce_specversion")).isEqualTo("1.0");
        assertThat(ObservadorDeTopico.cabecalho(eventoCancelamento, "ce_id")).isNotBlank();
        assertThat(ObservadorDeTopico.cabecalho(eventoCancelamento, "ce_source")).isEqualTo("/loja/servico-estoque");
        assertThat(ObservadorDeTopico.cabecalho(eventoCancelamento, "ce_type"))
                .isEqualTo("loja.estoque.reservacancelada.v1");
        assertThat(ObservadorDeTopico.cabecalho(eventoCancelamento, "ce_time")).contains("T").endsWith("Z");
        assertThat(eventoCancelamento.value())
                .contains("\"pedidoId\":\"7001\"")
                .contains("\"sku\":\"TEC-002\"")
                .contains("\"quantidade\":4")
                .contains("\"ocorridoEm\":\"");
    }

    @Test
    @DisplayName("2 - a mesma recusa entregue duas vezes devolve o estoque uma vez so e publica um so evento")
    void mesmaRecusaEntregueDuasVezesEhIdempotente() {
        String pedidoId = "7002";
        publicador.publicar(UUID.randomUUID().toString(), pedidoId, SKU, 3);
        aguardarEstoque(97);

        String recusaId = UUID.randomUUID().toString();
        publicador.publicarRecusa(topicoPagamentoRecusado, recusaId, pedidoId, "saldo insuficiente");
        aguardarEstoque(100);
        cancelamentos.aguardarComChave(pedidoId);

        publicador.publicarRecusa(topicoPagamentoRecusado, recusaId, pedidoId, "saldo insuficiente");
        publicador.publicarRecusa(topicoPagamentoRecusado, UUID.randomUUID().toString(), pedidoId, "outra tentativa");

        Awaitility.await()
                .during(JANELA_DE_OBSERVACAO)
                .atMost(PRAZO)
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> {
                    assertThat(repositorio.quantidadeEmEstoque(SKU)).isEqualTo(100);
                    assertThat(cancelamentos.comChave(pedidoId)).hasSize(1);
                });
    }

    @Test
    @DisplayName("3 - recusa de um pedido sem reserva nao altera o estoque e nao publica cancelamento")
    void recusaSemReservaEhIgnoradaComSeguranca() {
        String pedidoId = "7003-sem-reserva";
        String recusaId = UUID.randomUUID().toString();

        publicador.publicarRecusa(topicoPagamentoRecusado, recusaId, pedidoId, "pedido nunca reservou");

        Awaitility.await()
                .atMost(PRAZO)
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> assertThat(repositorio.contarEventosProcessados()).isEqualTo(1L));

        assertThat(repositorio.quantidadeEmEstoque(SKU)).isEqualTo(100);
        assertThat(cancelamentos.comChave(pedidoId)).isEmpty();
    }

    @Test
    @DisplayName("4 - se cair depois de devolver o estoque e antes de publicar o evento, a reentrega publica sem devolver duas vezes")
    void recuperaPublicacaoPendenteAposFalhaNoMeioDaCompensacao() {
        String pedidoId = "7004";
        publicador.publicar(UUID.randomUUID().toString(), pedidoId, SKU, 5);
        aguardarEstoque(95);

        doThrow(new DataAccessResourceFailureException("broker indisponivel antes de publicar"))
                .doCallRealMethod()
                .when(publicadorDeCancelamento).publicar(any());

        String recusaId = UUID.randomUUID().toString();
        publicador.publicarRecusa(topicoPagamentoRecusado, recusaId, pedidoId, "falha no meio");

        ConsumerRecord<String, String> eventoCancelamento = cancelamentos.aguardarComChave(pedidoId);
        assertThat(repositorio.quantidadeEmEstoque(SKU)).isEqualTo(100);
        assertThat(cancelamentos.comChave(pedidoId)).hasSize(1);
        assertThat(eventoCancelamento.value()).contains("\"quantidade\":5");
    }

    private void aguardarEstoque(int quantidadeEsperada) {
        EstoqueVerifier verificador = new EstoqueVerifier(repositorio, SKU, quantidadeEsperada);
        Awaitility.await()
                .atMost(PRAZO)
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(verificador);
    }
}