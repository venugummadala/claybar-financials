package com.claybar.notification;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnMissingBean(value = NotificationSender.class, ignored = LoggingNotificationSender.class)
public class LoggingNotificationSender implements NotificationSender {

    private static final Logger log = LoggerFactory.getLogger(LoggingNotificationSender.class);

    @Override
    public void send(Notification n) {
        log.info("[{}] to={} subject=\"{}\" body=\"{}\"", n.channel(), n.recipient(), n.subject(), n.body());
    }
}
