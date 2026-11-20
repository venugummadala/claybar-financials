package com.claybar.workflow;

import org.camunda.bpm.engine.impl.history.event.HistoryEvent;
import org.camunda.bpm.engine.impl.history.handler.HistoryEventHandler;

import java.util.List;
import java.util.function.Supplier;

/** Defers creation of the real handler until the first event, after the application context is ready. */
class LazyHistoryEventHandler implements HistoryEventHandler {

    private final Supplier<HistoryEventHandler> factory;
    private volatile HistoryEventHandler delegate;

    LazyHistoryEventHandler(Supplier<HistoryEventHandler> factory) {
        this.factory = factory;
    }

    private HistoryEventHandler delegate() {
        HistoryEventHandler d = delegate;
        if (d == null) {
            synchronized (this) {
                if (delegate == null) {
                    delegate = factory.get();
                }
                d = delegate;
            }
        }
        return d;
    }

    @Override
    public void handleEvent(HistoryEvent historyEvent) {
        delegate().handleEvent(historyEvent);
    }

    @Override
    public void handleEvents(List<HistoryEvent> historyEvents) {
        delegate().handleEvents(historyEvents);
    }
}
