package br.pucminas.aed.estoque.domain;

public class SkuInexistenteException extends FalhaPermanenteException {

    private final String sku;

    public SkuInexistenteException(String sku) {
        super("SKU inexistente no estoque: " + sku);
        this.sku = sku;
    }

    public String getSku() {
        return sku;
    }
}