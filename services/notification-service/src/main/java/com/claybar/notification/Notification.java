package com.claybar.notification;

public record Notification(Channel channel, String recipient, String subject, String body, String correlationId) {

    public enum Channel { EMAIL, OPS_ALERT }
}
