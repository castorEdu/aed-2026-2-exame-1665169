package br.pucminas.aed.estoque;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.awaitility.Awaitility;

public class ObservadorDeTopico {

    private final String servidores;
    private final String topico;

    public ObservadorDeTopico(String servidores, String topico) {
        this.servidores = servidores;
        this.topico = topico;
    }

    public List<ConsumerRecord<String, String>> lerTudo() {
        Map<String, Object> config = new HashMap<String, Object>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servidores);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "observador-" + UUID.randomUUID());
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        List<ConsumerRecord<String, String>> registros = new ArrayList<ConsumerRecord<String, String>>();
        try (KafkaConsumer<String, String> consumidor = new KafkaConsumer<String, String>(config)) {
            List<TopicPartition> particoes = consumidor.partitionsFor(topico).stream()
                    .map(info -> new TopicPartition(info.topic(), info.partition()))
                    .toList();
            consumidor.assign(particoes);
            consumidor.seekToBeginning(particoes);
            Map<TopicPartition, Long> fim = consumidor.endOffsets(particoes);
            int vazias = 0;
            while (!terminou(consumidor, particoes, fim) && vazias < 5) {
                var lote = consumidor.poll(Duration.ofMillis(500));
                if (lote.isEmpty()) {
                    vazias++;
                }
                lote.forEach(registros::add);
            }
        }
        return registros;
    }

    public List<ConsumerRecord<String, String>> comCeId(String ceId) {
        List<ConsumerRecord<String, String>> encontrados = new ArrayList<ConsumerRecord<String, String>>();
        for (ConsumerRecord<String, String> registro : lerTudo()) {
            if (ceId.equals(cabecalho(registro, "ce_id"))) {
                encontrados.add(registro);
            }
        }
        return encontrados;
    }

    public ConsumerRecord<String, String> aguardarComCeId(String ceId) {
        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(500))
                .until(() -> !comCeId(ceId).isEmpty());
        return comCeId(ceId).get(0);
    }

    public static String cabecalho(ConsumerRecord<String, String> registro, String nome) {
        Header cabecalho = registro.headers().lastHeader(nome);
        if (cabecalho == null) {
            return null;
        }
        return new String(cabecalho.value(), UTF_8);
    }

    private boolean terminou(KafkaConsumer<String, String> consumidor, List<TopicPartition> particoes,
                             Map<TopicPartition, Long> fim) {
        for (TopicPartition particao : particoes) {
            if (consumidor.position(particao) < fim.get(particao)) {
                return false;
            }
        }
        return true;
    }
}