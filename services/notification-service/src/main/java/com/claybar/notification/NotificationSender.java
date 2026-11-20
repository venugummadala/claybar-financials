package com.claybar.notification;

/** Delivery port. Swap the logging implementation for SES / SendGrid / Slack / PagerDuty adapters. */
public interface NotificationSender {

    void send(Notification notification);
}
