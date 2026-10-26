package br.pucminas.aed.estoque.config;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.RetryListener;

public class RegistroDeFalhas implements RetryListener {

    private static final Logger log = LoggerFactory.getLogger(RegistroDeFalhas.class);

    private final String topicoDaDlq;

    public RegistroDeFalhas(String topicoDaDlq) {
        this.topicoDaDlq = topicoDaDlq;
    }

    @Override
    public void failedDelivery(ConsumerRecord<?, ?> registro, Exception falha, int tentativa) {
        log.warn("falha na execucao {}  chave={}  offset={}  causa={}",
                tentativa, registro.key(), registro.offset(), descrever(falha));
    }

    @Override
    public void recovered(ConsumerRecord<?, ?> registro, Exception falha) {
        log.error("evento enviado para {}  chave={}  offset={}  causa={}",
                topicoDaDlq, registro.key(), registro.offset(), descrever(falha));
    }

    @Override
    public void recoveryFailed(ConsumerRecord<?, ?> registro, Exception original, Exception falhaDaRecuperacao) {
        log.error("falha ao enviar para {}  chave={}  offset={}", topicoDaDlq, registro.key(), registro.offset(),
                falhaDaRecuperacao);
    }

    private String descrever(Throwable falha) {
        Throwable atual = falha;
        while (atual.getCause() != null && atual.getClass().getName().startsWith("org.springframework.kafka")) {
            atual = atual.getCause();
        }
        return atual.getClass().getSimpleName() + ": " + atual.getMessage();
    }
}