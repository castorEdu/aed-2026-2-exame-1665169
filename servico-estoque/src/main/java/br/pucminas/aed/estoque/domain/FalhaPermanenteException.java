package br.pucminas.aed.estoque.domain;

public abstract class FalhaPermanenteException extends RuntimeException {

    protected FalhaPermanenteException(String mensagem) {
        super(mensagem);
    }

    protected FalhaPermanenteException(String mensagem, Throwable causa) {
        super(mensagem, causa);
    }
}