package org.mskcc.cmo.metadb;

import java.util.concurrent.CountDownLatch;
import org.mskcc.cmo.messaging.Gateway;
import org.mskcc.cmo.metadb.service.MessageHandlingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {"org.mskcc.cmo.common.*", "org.mskcc.cmo.messaging"})
public class ConsistencyCheckerApp implements CommandLineRunner {

    @Autowired
    private Gateway messagingGateway;

    @Autowired
    private MessageHandlingService messageHandlingService;

    private Thread shutdownHook;
    final CountDownLatch consistencyCheckerAppClose = new CountDownLatch(1);


    @Override
    public void run(String... args) throws Exception {
        System.out.println("Starting up MetaDB application...");
        try {
            installShutdownHook();
            messagingGateway.connect();
            messageHandlingService.initialize(messagingGateway);
            consistencyCheckerAppClose.await();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
        }
    }

    private void installShutdownHook() {
        shutdownHook =
            new Thread() {
                public void run() {
                    System.err.printf("\nCaught CTRL-C, shutting down gracefully...\n");
                    try {
                        messagingGateway.shutdown();
                        messageHandlingService.shutdown();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    consistencyCheckerAppClose.countDown();
                }
            };
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    public static void main(String[] args) {
        SpringApplication.run(ConsistencyCheckerApp.class, args);
    }

}
