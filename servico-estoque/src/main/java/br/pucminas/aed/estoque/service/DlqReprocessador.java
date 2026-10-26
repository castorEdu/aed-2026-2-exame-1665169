package br.pucminas.aed.estoque.service;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import br.pucminas.aed.estoque.config.CabecalhosDaDlq;

@Component
public class DlqReprocessador {

    private static final Logger log = LoggerFactory.getLogger(DlqReprocessador.class);

    private static final Duration ESPERA_DA_LEITURA = Duration.ofSeconds(2);
    private static final int LEITURAS_VAZIAS_ACEITAS = 5;
    private static final long PRAZO_DA_PUBLICACAO_SEGUNDOS = 15;

    private final KafkaProperties propriedades;
    private final KafkaTemplate<String, String> template;
    private final String topicoOriginal;
    private final String topicoDaDlq;
    private final String grupo;

    public DlqReprocessador(KafkaProperties propriedades,
                            KafkaTemplate<String, String> template,
                            @Value("${demo.topico}") String topicoOriginal,
                            @Value("${demo.topico-dlq}") String topicoDaDlq,
                            @Value("${demo.dlq.grupo-reprocessador}") String grupo) {
        this.propriedades = propriedades;
        this.template = template;
        this.topicoOriginal = topicoOriginal;
        this.topicoDaDlq = topicoDaDlq;
        this.grupo = grupo;
    }

    public int reprocessar() {
        try (KafkaConsumer<String, String> consumidor = new KafkaConsumer<String, String>(configuracao())) {
            List<TopicPartition> particoes = consumidor.partitionsFor(topicoDaDlq).stream()
                    .map(info -> new TopicPartition(info.topic(), info.partition()))
                    .toList();
            if (particoes.isEmpty()) {
                return 0;
            }
            consumidor.assign(particoes);
            posicionarNoPrimeiroNaoReprocessado(consumidor, particoes);
            Map<TopicPartition, Long> fim = consumidor.endOffsets(particoes);

            int republicados = 0;
            int leiturasVazias = 0;
            while (!chegouAoFim(consumidor, particoes, fim) && leiturasVazias < LEITURAS_VAZIAS_ACEITAS) {
                ConsumerRecords<String, String> lote = consumidor.poll(ESPERA_DA_LEITURA);
                if (lote.isEmpty()) {
                    leiturasVazias++;
                    continue;
                }
                leiturasVazias = 0;
                for (ConsumerRecord<String, String> registro : lote) {
                    republicar(registro);
                    republicados++;
                }
                consumidor.commitSync();
            }
            return republicados;
        }
    }

    private Map<String, Object> configuracao() {
        Map<String, Object> config = propriedades.buildConsumerProperties(null);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, grupo);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return config;
    }

    private void posicionarNoPrimeiroNaoReprocessado(KafkaConsumer<String, String> consumidor,
                                                     List<TopicPartition> particoes) {
        Map<TopicPartition, OffsetAndMetadata> confirmados = consumidor.committed(new HashSet<TopicPartition>(particoes));
        for (TopicPartition particao : particoes) {
            OffsetAndMetadata confirmado = confirmados.get(particao);
            if (confirmado == null) {
                consumidor.seekToBeginning(List.of(particao));
            } else {
                consumidor.seek(particao, confirmado.offset());
            }
        }
    }

    private boolean chegouAoFim(KafkaConsumer<String, String> consumidor,
                                List<TopicPartition> particoes,
                                Map<TopicPartition, Long> fim) {
        for (TopicPartition particao : particoes) {
            if (consumidor.position(particao) < fim.get(particao)) {
                return false;
            }
        }
        return true;
    }

    private void republicar(ConsumerRecord<String, String> registro) {
        ProducerRecord<String, String> saida =
                new ProducerRecord<String, String>(topicoOriginal, registro.key(), registro.value());
        for (Header cabecalho : registro.headers()) {
            if (cabecalho.key().startsWith(CabecalhosDaDlq.PREFIXO_CLOUDEVENTS)) {
                saida.headers().add(cabecalho.key(), cabecalho.value());
            }
        }
        try {
            template.send(saida).get(PRAZO_DA_PUBLICACAO_SEGUNDOS, TimeUnit.SECONDS);
        } catch (InterruptedException erro) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("reprocessamento interrompido", erro);
        } catch (ExecutionException | TimeoutException erro) {
            throw new IllegalStateException("falha ao republicar o evento da DLQ", erro);
        }
        log.info("DLQ -> {}  chave={}  offset-na-dlq={}", topicoOriginal, registro.key(), registro.offset());
    }
}