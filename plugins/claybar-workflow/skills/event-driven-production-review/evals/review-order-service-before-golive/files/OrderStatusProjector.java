package com.example.orders;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class OrderStatusProjector {

    private final OrderRepository orders;
    private final EventCodec codec;

    public OrderStatusProjector(OrderRepository orders, EventCodec codec) {
        this.orders = orders;
        this.codec = codec;
    }

    @KafkaListener(topics = "orders.shipping.events")
    public void onShipped(ConsumerRecord<String, String> record) {
        apply(codec.decode(record.value()));
    }

    @Transactional
    public void apply(EventEnvelope event) {
        orders.findById(event.correlationId()).ifPresent(order -> order.setStatus(OrderStatus.SHIPPED));
    }
}
