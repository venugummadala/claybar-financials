package com.claybar.orchestrator;

import org.camunda.bpm.engine.rest.security.auth.ProcessEngineAuthenticationFilter;
import org.camunda.bpm.engine.rest.security.auth.impl.HttpBasicAuthenticationProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * The Camunda REST API is <b>unauthenticated by default</b> in the Spring Boot starter. This enables
 * HTTP Basic against the engine's identity service (and with it Camunda authorization checks).
 * Behind an API gateway / SSO replace the provider with an OAuth2/JWT one.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "claybar.security.engine-rest", name = "basic-auth", havingValue = "true", matchIfMissing = true)
public class EngineRestSecurityConfiguration {

    @Bean
    public FilterRegistrationBean<ProcessEngineAuthenticationFilter> engineRestAuthenticationFilter() {
        FilterRegistrationBean<ProcessEngineAuthenticationFilter> registration =
                new FilterRegistrationBean<>(new ProcessEngineAuthenticationFilter());
        registration.setName("camunda-engine-rest-auth");
        registration.addInitParameter("authentication-provider", HttpBasicAuthenticationProvider.class.getName());
        registration.addUrlPatterns("/engine-rest/*");
        return registration;
    }
}
