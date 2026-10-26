package br.pucminas.aed.estoque.config;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.time.Instant;
import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

import br.pucminas.aed.estoque.domain.FalhaPermanenteException;
import br.pucminas.aed.estoque.service.ClassificadorDeFalhas;

@Configuration
public class ResilienciaConfig {

    private static final int PARTICOES_DA_DLQ = 3;
    private static final short REPLICAS_DA_DLQ = 1;

    @Bean
    public NewTopic topicoDaDlq(@Value("${demo.topico-dlq}") String topicoDaDlq) {
        return new NewTopic(topicoDaDlq, PARTICOES_DA_DLQ, REPLICAS_DA_DLQ);
    }

    @Bean
    public NewTopic topicoDePagamentoRecusado(@Value("${demo.topico-pagamento-recusado}") String topico) {
        return new NewTopic(topico, PARTICOES_DA_DLQ, REPLICAS_DA_DLQ);
    }

    @Bean
    public NewTopic topicoDeReservaCancelada(@Value("${demo.topico-reserva-cancelada}") String topico) {
        return new NewTopic(topico, PARTICOES_DA_DLQ, REPLICAS_DA_DLQ);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(KafkaProperties propriedades) {
        Map<String, Object> config = propriedades.buildProducerProperties(null);
        config.remove(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
        config.remove(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
        DefaultKafkaProducerFactory<String, String> fabrica =
                new DefaultKafkaProducerFactory<String, String>(config, new StringSerializer(), new StringSerializer());
        return new KafkaTemplate<String, String>(fabrica);
    }

    @Bean
    public DeadLetterPublishingRecoverer recuperadorDaDlq(KafkaTemplate<String, String> template,
                                                          @Value("${demo.topico-dlq}") String topicoDaDlq) {
        DeadLetterPublishingRecoverer recuperador = new DeadLetterPublishingRecoverer(template,
                (registro, falha) -> new TopicPartition(topicoDaDlq, registro.partition()));
        recuperador.setHeadersFunction((registro, falha) -> cabecalhosDoMotivo(falha));
        return recuperador;
    }

    @Bean
    public DefaultErrorHandler tratadorDeErros(DeadLetterPublishingRecoverer recuperador,
            @Value("${demo.topico-dlq}") String topicoDaDlq,
            @Value("${demo.retentativa.max-retentativas}") int maxRetentativas,
            @Value("${demo.retentativa.espera-inicial-ms}") long esperaInicialMs,
            @Value("${demo.retentativa.multiplicador}") double multiplicador,
            @Value("${demo.retentativa.espera-maxima-ms}") long esperaMaximaMs,
            @Value("${demo.retentativa.espera-fixa-ms}") long esperaFixaMs) {

        ExponentialBackOff exponencial = new ExponentialBackOff(esperaInicialMs, multiplicador);
        exponencial.setMaxInterval(esperaMaximaMs);
        exponencial.setMaxAttempts(maxRetentativas);

        FixedBackOff fixa = new FixedBackOff(esperaFixaMs, maxRetentativas);

        DefaultErrorHandler tratador = new DefaultErrorHandler(recuperador, fixa);
        tratador.setBackOffFunction((registro, falha) -> escolherEspera(falha, exponencial, fixa));
        tratador.addNotRetryableExceptions(FalhaPermanenteException.class);
        tratador.setCommitRecovered(true);
        tratador.setLogLevel(KafkaException.Level.DEBUG);
        tratador.setRetryListeners(new RegistroDeFalhas(topicoDaDlq));
        return tratador;
    }

    private BackOff escolherEspera(Exception falha, BackOff exponencial, BackOff fixa) {
        if (ClassificadorDeFalhas.ehTransitoria(falha)) {
            return exponencial;
        }
        return fixa;
    }

    private Headers cabecalhosDoMotivo(Exception falha) {
        Throwable causa = causaRaiz(falha);
        String mensagem = causa.getMessage();
        if (mensagem == null) {
            mensagem = "";
        }
        RecordHeaders cabecalhos = new RecordHeaders();
        cabecalhos.add(CabecalhosDaDlq.EXCECAO, causa.getClass().getName().getBytes(UTF_8));
        cabecalhos.add(CabecalhosDaDlq.MOTIVO, mensagem.getBytes(UTF_8));
        cabecalhos.add(CabecalhosDaDlq.FALHOU_EM, Instant.now().toString().getBytes(UTF_8));
        return cabecalhos;
    }

    private Throwable causaRaiz(Throwable falha) {
        Throwable atual = falha;
        while (atual instanceof ListenerExecutionFailedException && atual.getCause() != null) {
            atual = atual.getCause();
        }
        return atual;
    }
}