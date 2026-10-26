package br.pucminas.aed.estoque.service;

import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;

import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.transaction.CannotCreateTransactionException;

import br.pucminas.aed.estoque.domain.FalhaPermanenteException;

public final class ClassificadorDeFalhas {

    private ClassificadorDeFalhas() {
    }

    public static boolean ehPermanente(Throwable falha) {
        return percorrer(falha, FalhaPermanenteException.class);
    }

    public static boolean ehTransitoria(Throwable falha) {
        return percorrer(falha, TransientDataAccessException.class)
                || percorrer(falha, DataAccessResourceFailureException.class)
                || percorrer(falha, CannotCreateTransactionException.class)
                || percorrer(falha, SQLTransientException.class)
                || percorrer(falha, SQLRecoverableException.class);
    }

    private static boolean percorrer(Throwable falha, Class<? extends Throwable> tipo) {
        Throwable atual = falha;
        int profundidade = 0;
        while (atual != null && profundidade < 20) {
            if (tipo.isInstance(atual)) {
                return true;
            }
            atual = atual.getCause();
            profundidade++;
        }
        return false;
    }
}