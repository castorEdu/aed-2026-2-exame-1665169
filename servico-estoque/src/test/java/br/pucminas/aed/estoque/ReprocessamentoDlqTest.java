package br.pucminas.aed.estoque;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource;

import br.pucminas.aed.estoque.service.DlqReprocessador;
import br.pucminas.aed.estoque.service.EstoqueRepository;

@SpringBootTest
@EmbeddedKafka(partitions = 3, topics = {"loja.pedido.confirmado.v1", "loja.pedido.confirmado.v1.dlq"})
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.datasource.url=jdbc:h2:mem:aed_reprocessamento;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "demo.retentativa.espera-inicial-ms=100",
        "demo.retentativa.espera-maxima-ms=400",
        "demo.retentativa.espera-fixa-ms=100",
        "logging.level.br.pucminas.aed=INFO"
})
class ReprocessamentoDlqTest {

    private static final String SKU_NOVO = "TEC-777";
    private static final Duration PRAZO = Duration.ofSeconds(30);

    @Autowired
    private EstoqueRepository repositorio;
    @Autowired
    private JdbcTemplate jdbc;
    @Autowired
    private DlqReprocessador reprocessador;

    @Value("${spring.embedded.kafka.brokers}")
    private String servidores;
    @Value("${demo.topico}")
    private String topico;
    @Value("${demo.topico-dlq}")
    private String topicoDaDlq;

    @Test
    @DisplayName("6 - corrigida a causa, o evento da DLQ volta ao fluxo com o mesmo ce_id")
    void eventoDaDlqVoltaAoFluxoNormal() {
        String ceId = UUID.randomUUID().toString();
        TestPublisher publicador = new TestPublisher(servidores, topico);
        ObservadorDeTopico dlq = new ObservadorDeTopico(servidores, topicoDaDlq);
        ObservadorDeTopico principal = new ObservadorDeTopico(servidores, topico);

        publicador.publicar(ceId, "6001", SKU_NOVO, 2);
        dlq.aguardarComCeId(ceId);
        assertThat(principal.comCeId(ceId)).hasSize(1);

        jdbc.update("INSERT INTO estoque (sku, quantidade) VALUES (?, ?)", SKU_NOVO, 10);

        int republicados = reprocessador.reprocessar();

        assertThat(republicados).isEqualTo(1);
        Awaitility.await().atMost(PRAZO).pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> assertThat(repositorio.quantidadeEmEstoque(SKU_NOVO)).isEqualTo(8));

        assertThat(principal.comCeId(ceId)).hasSize(2);
        for (ConsumerRecord<String, String> registro : principal.comCeId(ceId)) {
            assertThat(ObservadorDeTopico.cabecalho(registro, "ce_id")).isEqualTo(ceId);
            assertThat(ObservadorDeTopico.cabecalho(registro, "dlq_motivo")).isNull();
        }
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM evento_processado WHERE evento_id = ?",
                Integer.class, ceId)).isEqualTo(1);
    }

    @Test
    @DisplayName("7 - uma segunda execucao do reprocessamento nao republica o que ja foi reprocessado")
    void reprocessamentoNaoRepeteOQueJaFoiRepublicado() {
        assertThat(reprocessador.reprocessar()).isGreaterThanOrEqualTo(0);
        assertThat(reprocessador.reprocessar()).isZero();
    }
}