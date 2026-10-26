package br.pucminas.aed.estoque.config;

public final class CabecalhosDaDlq {

    public static final String EXCECAO = "dlq_excecao";
    public static final String MOTIVO = "dlq_motivo";
    public static final String FALHOU_EM = "dlq_falhou_em";
    public static final String PREFIXO_CLOUDEVENTS = "ce_";

    private CabecalhosDaDlq() {
    }
}