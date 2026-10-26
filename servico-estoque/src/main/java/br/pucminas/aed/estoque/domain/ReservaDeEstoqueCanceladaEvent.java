package br.pucminas.aed.estoque.domain;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public final class ReservaDeEstoqueCanceladaEvent {

    private final String eventoId;
    private final Instant ocorridoEm;
    private final String pedidoId;
    private final List<ItemDoPedidoVO> itens;

    @JsonCreator
    public ReservaDeEstoqueCanceladaEvent(@JsonProperty("eventoId") String eventoId,
                                          @JsonProperty("ocorridoEm") Instant ocorridoEm,
                                          @JsonProperty("pedidoId") String pedidoId,
                                          @JsonProperty("itens") List<ItemDoPedidoVO> itens) {
        this.eventoId = Objects.requireNonNull(eventoId, "eventoId e obrigatorio");
        this.ocorridoEm = Objects.requireNonNull(ocorridoEm, "ocorridoEm e obrigatorio");
        this.pedidoId = Objects.requireNonNull(pedidoId, "pedidoId e obrigatorio");

        List<ItemDoPedidoVO> copia = new ArrayList<ItemDoPedidoVO>();
        if (itens != null) {
            copia.addAll(itens);
        }
        this.itens = Collections.unmodifiableList(copia);
    }

    public String getEventoId() {
        return eventoId;
    }

    public Instant getOcorridoEm() {
        return ocorridoEm;
    }

    public String getPedidoId() {
        return pedidoId;
    }

    public List<ItemDoPedidoVO> getItens() {
        return itens;
    }

    @Override
    public boolean equals(Object outro) {
        if (this == outro) {
            return true;
        }
        if (!(outro instanceof ReservaDeEstoqueCanceladaEvent)) {
            return false;
        }
        ReservaDeEstoqueCanceladaEvent aquele = (ReservaDeEstoqueCanceladaEvent) outro;
        return this.eventoId.equals(aquele.eventoId);
    }

    @Override
    public int hashCode() {
        return eventoId.hashCode();
    }

    @Override
    public String toString() {
        return "ReservaDeEstoqueCanceladaEvent{eventoId=" + eventoId
                + ", pedidoId=" + pedidoId
                + ", ocorridoEm=" + ocorridoEm
                + ", itens=" + itens.size() + "}";
    }
}
