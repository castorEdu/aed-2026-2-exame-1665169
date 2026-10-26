package br.pucminas.aed.estoque.service;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import br.pucminas.aed.estoque.domain.EventoMalformadoException;
import br.pucminas.aed.estoque.domain.PedidoConfirmadoEvent;

@Component
public class PedidoConfirmadoLeitor {

    private final ObjectMapper mapeador = JsonMapper.builder()
            .addModule(new JavaTimeModule())
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    public PedidoConfirmadoEvent ler(String json) {
        if (json == null || json.isBlank()) {
            throw new EventoMalformadoException("evento sem corpo");
        }
        try {
            return mapeador.readValue(json, PedidoConfirmadoEvent.class);
        } catch (JsonProcessingException erro) {
            throw new EventoMalformadoException("corpo do evento invalido: " + erro.getOriginalMessage(), erro);
        }
    }
}