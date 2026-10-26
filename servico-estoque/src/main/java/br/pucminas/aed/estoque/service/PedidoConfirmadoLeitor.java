package br.pucminas.aed.estoque.service;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import br.pucminas.aed.estoque.domain.EventoMalformadoException;
import br.pucminas.aed.estoque.domain.PagamentoRecusadoEvent;
import br.pucminas.aed.estoque.domain.PedidoConfirmadoEvent;
import br.pucminas.aed.estoque.domain.ReservaDeEstoqueCanceladaEvent;

@Component
public class PedidoConfirmadoLeitor {

    private final ObjectMapper mapeador = JsonMapper.builder()
            .addModule(new JavaTimeModule())
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .build();

    public PedidoConfirmadoEvent ler(String json) {
        return lerValor(json, PedidoConfirmadoEvent.class);
    }

    public PagamentoRecusadoEvent lerPagamentoRecusado(String json) {
        return lerValor(json, PagamentoRecusadoEvent.class);
    }

    public String escrever(ReservaDeEstoqueCanceladaEvent evento) {
        try {
            return mapeador.writeValueAsString(evento);
        } catch (JsonProcessingException erro) {
            throw new IllegalStateException("falha ao serializar ReservaDeEstoqueCanceladaEvent", erro);
        }
    }

    private <T> T lerValor(String json, Class<T> tipo) {
        if (json == null || json.isBlank()) {
            throw new EventoMalformadoException("evento sem corpo");
        }
        try {
            return mapeador.readValue(json, tipo);
        } catch (JsonProcessingException erro) {
            throw new EventoMalformadoException("corpo do evento invalido: " + erro.getOriginalMessage(), erro);
        }
    }
}