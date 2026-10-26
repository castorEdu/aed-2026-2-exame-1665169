package br.pucminas.aed.estoque.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Component;

import br.pucminas.aed.estoque.service.DlqReprocessador;

@Component
@ConditionalOnProperty(name = "demo.reprocessar-dlq", havingValue = "true")
public class ReprocessarDlqRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(ReprocessarDlqRunner.class);

    private final DlqReprocessador reprocessador;
    private final ConfigurableApplicationContext contexto;

    public ReprocessarDlqRunner(DlqReprocessador reprocessador, ConfigurableApplicationContext contexto) {
        this.reprocessador = reprocessador;
        this.contexto = contexto;
    }

    @Override
    public void run(ApplicationArguments argumentos) {
        int total = reprocessador.reprocessar();
        log.info("reprocessamento concluido: {} evento(s) republicado(s)", total);
        SpringApplication.exit(contexto, () -> 0);
    }
}