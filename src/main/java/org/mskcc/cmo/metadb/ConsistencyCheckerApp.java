package org.mskcc.cmo.metadb;

import org.mskcc.cmo.metadb.util.ConsistencyCheckerUtil;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {"org.mskcc.cmo.common.*"})
public class ConsistencyCheckerApp implements CommandLineRunner {

    // will configure this to autowire properly later..
    private ConsistencyCheckerUtil util = new ConsistencyCheckerUtil();

    public static void main(String[] args) {
        SpringApplication.run(ConsistencyCheckerApp.class, args);
    }

    @Override
    public void run(String... args) throws Exception {}
}
