package com.claybar.workflow;

import org.camunda.bpm.engine.impl.cfg.AbstractProcessEnginePlugin;
import org.camunda.bpm.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.camunda.bpm.engine.impl.history.handler.HistoryEventHandler;

import java.util.ArrayList;
import java.util.List;

/**
 * Camunda process engine plugin that registers {@link KafkaLifecycleHistoryEventHandler} next to the
 * default database history handler. Because it is a standard {@code ProcessEnginePlugin} it also
 * works outside Spring Boot (e.g. in a {@code bpm-platform.xml} / shared engine) when constructed
 * manually.
 */
public class ClaybarKafkaEventsProcessEnginePlugin extends AbstractProcessEnginePlugin {

    private final HistoryEventHandler handler;

    public ClaybarKafkaEventsProcessEnginePlugin(HistoryEventHandler handler) {
        this.handler = handler;
    }

    @Override
    public void preInit(ProcessEngineConfigurationImpl configuration) {
        List<HistoryEventHandler> handlers = configuration.getCustomHistoryEventHandlers() == null
                ? new ArrayList<>() : new ArrayList<>(configuration.getCustomHistoryEventHandlers());
        handlers.add(handler);
        configuration.setCustomHistoryEventHandlers(handlers);
        configuration.setEnableDefaultDbHistoryEventHandler(true);
    }

    @Override
    public String toString() {
        return "ClaybarKafkaEventsProcessEnginePlugin";
    }
}
