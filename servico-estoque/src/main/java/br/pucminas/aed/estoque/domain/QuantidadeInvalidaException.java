package br.pucminas.aed.estoque.domain;

public class QuantidadeInvalidaException extends FalhaPermanenteException {

    private final String sku;
    private final int quantidade;

    public QuantidadeInvalidaException(String sku, int quantidade) {
        super("Quantidade invalida para o SKU " + sku + ": " + quantidade);
        this.sku = sku;
        this.quantidade = quantidade;
    }

    public String getSku() {
        return sku;
    }

    public int getQuantidade() {
        return quantidade;
    }
}