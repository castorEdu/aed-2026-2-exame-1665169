package br.pucminas.aed.estoque.domain;

public class EventoMalformadoException extends FalhaPermanenteException {

    public EventoMalformadoException(String mensagem) {
        super(mensagem);
    }

    public EventoMalformadoException(String mensagem, Throwable causa) {
        super(mensagem, causa);
    }
}