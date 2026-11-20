package com.claybar.orchestrator;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class SlaConfiguration {

    /** Exposed under a short, stable bean name for BPMN expressions. */
    @Bean("loanSla")
    @ConfigurationProperties(prefix = "claybar.loan.sla")
    public LoanSlaProperties loanSla() {
        return new LoanSlaProperties();
    }
}
