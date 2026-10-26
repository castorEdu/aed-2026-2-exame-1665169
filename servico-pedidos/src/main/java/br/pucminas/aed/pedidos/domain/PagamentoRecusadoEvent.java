package br.pucminas.aed.pedidos.domain;

import java.time.Instant;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public final class PagamentoRecusadoEvent {

    private final String eventoId;
    private final Instant ocorridoEm;
    private final String pedidoId;
    private final String motivo;

    @JsonCreator
    public PagamentoRecusadoEvent(@JsonProperty("eventoId") String eventoId,
                                  @JsonProperty("ocorridoEm") Instant ocorridoEm,
                                  @JsonProperty("pedidoId") String pedidoId,
                                  @JsonProperty("motivo") String motivo) {
        this.eventoId = Objects.requireNonNull(eventoId, "eventoId e obrigatorio");
        this.ocorridoEm = Objects.requireNonNull(ocorridoEm, "ocorridoEm e obrigatorio");
        this.pedidoId = Objects.requireNonNull(pedidoId, "pedidoId e obrigatorio");
        this.motivo = motivo;
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

    public String getMotivo() {
        return motivo;
    }

    @Override
    public boolean equals(Object outro) {
        if (this == outro) {
            return true;
        }
        if (!(outro instanceof PagamentoRecusadoEvent)) {
            return false;
        }
        PagamentoRecusadoEvent aquele = (PagamentoRecusadoEvent) outro;
        return this.eventoId.equals(aquele.eventoId);
    }

    @Override
    public int hashCode() {
        return eventoId.hashCode();
    }

    @Override
    public String toString() {
        return "PagamentoRecusadoEvent{eventoId=" + eventoId
                + ", pedidoId=" + pedidoId
                + ", ocorridoEm=" + ocorridoEm
                + ", motivo=" + motivo + "}";
    }
}
