package br.pucminas.aed.estoque;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource;

import br.pucminas.aed.estoque.domain.QuantidadeInvalidaException;
import br.pucminas.aed.estoque.domain.SkuInexistenteException;
import br.pucminas.aed.estoque.service.EstoqueRepository;

@SpringBootTest
@EmbeddedKafka(partitions = 3, topics = {"loja.pedido.confirmado.v1", "loja.pedido.confirmado.v1.dlq"})
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.datasource.url=jdbc:h2:mem:aed_falhas;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
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
class CaminhoDeFalhaTest {

    private static final String SKU = "TEC-002";
    private static final int ESTOQUE_INICIAL = 100;
    private static final Duration PRAZO = Duration.ofSeconds(30);

    @SpyBean
    private EstoqueRepository repositorio;

    @Value("${spring.embedded.kafka.brokers}")
    private String servidores;
    @Value("${demo.topico}")
    private String topico;
    @Value("${demo.topico-dlq}")
    private String topicoDaDlq;
    @Value("${demo.retentativa.max-retentativas}")
    private int maxRetentativas;

    private TestPublisher publicador;
    private ObservadorDeTopico dlq;

    @BeforeEach
    void prepararEstado() {
        repositorio.redefinirEstoque(SKU, ESTOQUE_INICIAL);
        publicador = new TestPublisher(servidores, topico);
        dlq = new ObservadorDeTopico(servidores, topicoDaDlq);
    }

    @Test
    @DisplayName("1 - SKU inexistente vai para a DLQ sem retentativa, com ce_id e motivo")
    void skuInexistenteVaiDiretoParaDlq() {
        String ceId = UUID.randomUUID().toString();
        Instant antes = Instant.now();

        publicador.publicar(ceId, "5001", "TEC-404", 1);

        ConsumerRecord<String, String> naDlq = dlq.aguardarComCeId(ceId);

        assertThat(ObservadorDeTopico.cabecalho(naDlq, "ce_id")).isEqualTo(ceId);
        assertThat(ObservadorDeTopico.cabecalho(naDlq, "ce_type")).isEqualTo("loja.pedido.confirmado.v1");
        assertThat(ObservadorDeTopico.cabecalho(naDlq, "ce_specversion")).isEqualTo("1.0");
        assertThat(ObservadorDeTopico.cabecalho(naDlq, "ce_source")).isEqualTo("/loja/servico-pedidos");
        assertThat(ObservadorDeTopico.cabecalho(naDlq, "dlq_excecao")).isEqualTo(SkuInexistenteException.class.getName());
        assertThat(ObservadorDeTopico.cabecalho(naDlq, "dlq_motivo")).contains("TEC-404");
        assertThat(Instant.parse(ObservadorDeTopico.cabecalho(naDlq, "dlq_falhou_em"))).isAfterOrEqualTo(antes);
        assertThat(ObservadorDeTopico.cabecalho(naDlq, "kafka_dlt-original-topic")).isEqualTo(topico);
        assertThat(naDlq.value()).contains("\"clienteId\":\"c-9931\"");

        verify(repositorio, times(1)).reservar(eq("TEC-404"), anyInt());
        assertThat(repositorio.quantidadeEmEstoque(SKU)).isEqualTo(ESTOQUE_INICIAL);
    }

    @Test
    @DisplayName("2 - quantidade menor ou igual a zero vai direto para a DLQ")
    void quantidadeInvalidaVaiDiretoParaDlq() {
        String ceId = UUID.randomUUID().toString();

        publicador.publicar(ceId, "5002", SKU, 0);

        ConsumerRecord<String, String> naDlq = dlq.aguardarComCeId(ceId);

        assertThat(ObservadorDeTopico.cabecalho(naDlq, "ce_id")).isEqualTo(ceId);
        assertThat(ObservadorDeTopico.cabecalho(naDlq, "dlq_excecao")).isEqualTo(QuantidadeInvalidaException.class.getName());
        verify(repositorio, times(1)).reservar(SKU, 0);
        assertThat(repositorio.quantidadeEmEstoque(SKU)).isEqualTo(ESTOQUE_INICIAL);
    }

    @Test
    @DisplayName("3 - falha transitoria e retentada ate dar certo e o evento NAO vai para a DLQ")
    void falhaTransitoriaEhRetentada() {
        String ceId = UUID.randomUUID().toString();
        doThrow(new DataAccessResourceFailureException("postgres fora do ar"))
                .doThrow(new DataAccessResourceFailureException("postgres fora do ar"))
                .doCallRealMethod()
                .when(repositorio).reservar(SKU, 7);

        publicador.publicar(ceId, "5003", SKU, 7);

        Awaitility.await().atMost(PRAZO).pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> assertThat(repositorio.quantidadeEmEstoque(SKU)).isEqualTo(ESTOQUE_INICIAL - 7));

        verify(repositorio, times(3)).reservar(SKU, 7);
        assertThat(dlq.comCeId(ceId)).isEmpty();
    }

    @Test
    @DisplayName("4 - falha transitoria que nao passa esgota o limite e vai para a DLQ")
    void falhaTransitoriaEsgotaOLimite() {
        String ceId = UUID.randomUUID().toString();
        doThrow(new DataAccessResourceFailureException("postgres fora do ar"))
                .when(repositorio).reservar(SKU, 8);

        publicador.publicar(ceId, "5004", SKU, 8);

        ConsumerRecord<String, String> naDlq = dlq.aguardarComCeId(ceId);

        assertThat(ObservadorDeTopico.cabecalho(naDlq, "ce_id")).isEqualTo(ceId);
        assertThat(ObservadorDeTopico.cabecalho(naDlq, "dlq_excecao"))
                .isEqualTo(DataAccessResourceFailureException.class.getName());
        assertThat(ObservadorDeTopico.cabecalho(naDlq, "dlq_motivo")).contains("postgres fora do ar");
        verify(repositorio, times(1 + maxRetentativas)).reservar(SKU, 8);
        assertThat(repositorio.quantidadeEmEstoque(SKU)).isEqualTo(ESTOQUE_INICIAL);
    }

    @Test
    @DisplayName("5 - JSON invalido e falha permanente e vai para a DLQ com o corpo original")
    void eventoMalformadoVaiParaDlq() {
        String ceId = UUID.randomUUID().toString();

        publicador.publicarCru(ceId, "5005", "{isto nao e json");

        ConsumerRecord<String, String> naDlq = dlq.aguardarComCeId(ceId);

        assertThat(ObservadorDeTopico.cabecalho(naDlq, "dlq_excecao"))
                .isEqualTo("br.pucminas.aed.estoque.domain.EventoMalformadoException");
        assertThat(naDlq.value()).isEqualTo("{isto nao e json");
    }
}