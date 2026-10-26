package br.pucminas.aed.estoque.service;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import br.pucminas.aed.estoque.domain.ItemDoPedidoVO;
import br.pucminas.aed.estoque.domain.QuantidadeInvalidaException;
import br.pucminas.aed.estoque.domain.ReservaDeEstoqueCanceladaEvent;
import br.pucminas.aed.estoque.domain.SkuInexistenteException;

/**
 * Toda a persistencia deste servico, num lugar so.
 *
 * Sao duas tabelas com papeis bem diferentes, e vale separa-las na cabeca do
 * aluno:
 *
 *   evento_processado  memoria do que ja foi visto — sustenta a IDEMPOTENCIA
 *   estoque            o efeito de negocio — e o que nao pode acontecer 2 vezes
 *
 * Elas moram na mesma classe porque precisam morar na mesma transacao. E
 * justamente isso que fecha a janela em que o processo morreria entre registrar
 * a chave e aplicar o efeito.
 *
 * Repare que os metodos recebem sku e quantidade, e nao o evento inteiro. Um
 * repositorio nao deveria saber percorrer a lista de itens de um evento: isso e
 * orquestracao, e fica no EstoqueService.
 */
@Repository
public class EstoqueRepository {

    private final JdbcTemplate jdbc;

    public EstoqueRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    // ------------------------------------------------------------------
    // deduplicacao
    // ------------------------------------------------------------------

    /**
     * INSERT ... ON CONFLICT DO NOTHING devolve 1 quando a linha e nova e 0
     * quando ja existia. E isso, e so isso, que distingue "primeira vez" de
     * "reentrega".
     *
     * @return true se o evento e novo; false se ja foi processado antes.
     */
    public boolean registrarEventoSeNovo(String eventoId) {
        int linhas = jdbc.update(
                "INSERT INTO evento_processado (evento_id) VALUES (?) ON CONFLICT DO NOTHING",
                eventoId);
        return linhas == 1;
    }

    public long contarEventosProcessados() {
        Long total = jdbc.queryForObject("SELECT COUNT(*) FROM evento_processado", Long.class);
        if (total == null) {
            return 0L;
        }
        return total.longValue();
    }

    public void limparEventosProcessados() {
        jdbc.update("DELETE FROM evento_processado");
    }

    // ------------------------------------------------------------------
    // estoque
    // ------------------------------------------------------------------

    /** O efeito de negocio: baixar a quantidade reservada. */
    public void reservar(String sku, int quantidade) {
        if (quantidade <= 0) {
            throw new QuantidadeInvalidaException(sku, quantidade);
        }
        int linhas = jdbc.update("UPDATE estoque SET quantidade = quantidade - ? WHERE sku = ?",
                Integer.valueOf(quantidade), sku);
        if (linhas == 0) {
            throw new SkuInexistenteException(sku);
        }
    }

    public int quantidadeEmEstoque(String sku) {
        Integer quantidade = jdbc.queryForObject(
                "SELECT quantidade FROM estoque WHERE sku = ?", Integer.class, sku);
        if (quantidade == null) {
            return 0;
        }
        return quantidade.intValue();
    }

    /** Usado para reiniciar a demonstracao e nos testes. */
    public void redefinirEstoque(String sku, int quantidade) {
        jdbc.update("UPDATE estoque SET quantidade = ? WHERE sku = ?",
                Integer.valueOf(quantidade), sku);
    }

    public void registrarReserva(String pedidoId, String sku, int quantidade) {
        jdbc.update("INSERT INTO reserva (pedido_id, sku, quantidade) VALUES (?, ?, ?)",
                pedidoId, sku, Integer.valueOf(quantidade));
    }

    public void devolverEstoque(String sku, int quantidade) {
        int linhas = jdbc.update("UPDATE estoque SET quantidade = quantidade + ? WHERE sku = ?",
                Integer.valueOf(quantidade), sku);
        if (linhas == 0) {
            throw new SkuInexistenteException(sku);
        }
    }

    public List<ItemDoPedidoVO> buscarReservasAtivas(String pedidoId) {
        return jdbc.query(
                "SELECT sku, quantidade FROM reserva WHERE pedido_id = ? AND devolvida_em IS NULL ORDER BY id",
                (rs, numLinha) -> new ItemDoPedidoVO(rs.getString("sku"), rs.getInt("quantidade")),
                pedidoId);
    }

    public void marcarReservasDevolvidas(String pedidoId, String cancelamentoEventoId, Instant devolvidaEm) {
        jdbc.update(
                "UPDATE reserva SET devolvida_em = ?, cancelamento_evento_id = ? WHERE pedido_id = ? AND devolvida_em IS NULL",
                Timestamp.from(devolvidaEm), cancelamentoEventoId, pedidoId);
    }

    public ReservaDeEstoqueCanceladaEvent buscarCancelamentoPendente(String pedidoId) {
        List<String> metadados = jdbc.query(
                "SELECT cancelamento_evento_id, devolvida_em FROM reserva WHERE pedido_id = ? AND devolvida_em IS NOT NULL AND cancelamento_publicado_em IS NULL ORDER BY id",
                (rs, numLinha) -> rs.getString("cancelamento_evento_id") + "|" + rs.getTimestamp("devolvida_em").toInstant().toString(),
                pedidoId);
        if (metadados.isEmpty()) {
            return null;
        }
        String[] partes = metadados.get(0).split("\\|", 2);
        List<ItemDoPedidoVO> itens = jdbc.query(
                "SELECT sku, quantidade FROM reserva WHERE pedido_id = ? AND cancelamento_evento_id = ? ORDER BY id",
                (rs, numLinha) -> new ItemDoPedidoVO(rs.getString("sku"), rs.getInt("quantidade")),
                pedidoId, partes[0]);
        return new ReservaDeEstoqueCanceladaEvent(partes[0], Instant.parse(partes[1]), pedidoId, itens);
    }

    public void marcarCancelamentoPublicado(String pedidoId, String cancelamentoEventoId) {
        jdbc.update(
                "UPDATE reserva SET cancelamento_publicado_em = CURRENT_TIMESTAMP WHERE pedido_id = ? AND cancelamento_evento_id = ? AND cancelamento_publicado_em IS NULL",
                pedidoId, cancelamentoEventoId);
    }

    public void limparReservas() {
        jdbc.update("DELETE FROM reserva");
    }

    public long contarReservasAtivas(String pedidoId) {
        Long total = jdbc.queryForObject(
                "SELECT COUNT(*) FROM reserva WHERE pedido_id = ? AND devolvida_em IS NULL",
                Long.class, pedidoId);
        if (total == null) {
            return 0L;
        }
        return total.longValue();
    }

    public long contarReservasDevolvidas(String pedidoId) {
        Long total = jdbc.queryForObject(
                "SELECT COUNT(*) FROM reserva WHERE pedido_id = ? AND devolvida_em IS NOT NULL",
                Long.class, pedidoId);
        if (total == null) {
            return 0L;
        }
        return total.longValue();
    }
}
