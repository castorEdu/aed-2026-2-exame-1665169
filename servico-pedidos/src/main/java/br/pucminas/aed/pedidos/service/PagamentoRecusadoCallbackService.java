package br.pucminas.aed.pedidos.service;

import java.util.function.BiConsumer;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.SendResult;

import br.pucminas.aed.pedidos.domain.PagamentoRecusadoEvent;

public class PagamentoRecusadoCallbackService
        implements BiConsumer<SendResult<String, Object>, Throwable> {

    private static final Logger log = LoggerFactory.getLogger(PagamentoRecusadoCallbackService.class);

    private final PagamentoRecusadoEvent evento;

    public PagamentoRecusadoCallbackService(PagamentoRecusadoEvent evento) {
        this.evento = evento;
    }

    @Override
    public void accept(SendResult<String, Object> resultado, Throwable erro) {
        if (erro != null) {
            log.error("FALHA ao publicar evento {}", evento.getEventoId(), erro);
            return;
        }
        RecordMetadata metadados = resultado.getRecordMetadata();
        log.info("publicado  evento={}  pedido={}  particao={}  offset={}",
                evento.getEventoId(),
                evento.getPedidoId(),
                Integer.valueOf(metadados.partition()),
                Long.valueOf(metadados.offset()));
    }
}
