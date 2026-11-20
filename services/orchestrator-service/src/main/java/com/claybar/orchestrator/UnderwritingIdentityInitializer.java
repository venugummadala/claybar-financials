package com.claybar.orchestrator;

import org.camunda.bpm.engine.IdentityService;
import org.camunda.bpm.engine.identity.Group;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Seeds the {@code underwriters} candidate group and adds the admin user to it so the manual
 * underwriting task is claimable in Tasklist out of the box. In production identities come from
 * LDAP/SSO (camunda-identity-ldap plugin) and this initializer is disabled.
 */
@Component
@ConditionalOnProperty(prefix = "claybar.demo", name = "seed-identities", havingValue = "true")
public class UnderwritingIdentityInitializer {

    static final String UNDERWRITERS = "underwriters";
    private static final Logger log = LoggerFactory.getLogger(UnderwritingIdentityInitializer.class);

    private final IdentityService identityService;
    private final String adminUser;

    public UnderwritingIdentityInitializer(IdentityService identityService,
                                           @Value("${camunda.bpm.admin-user.id:admin}") String adminUser) {
        this.identityService = identityService;
        this.adminUser = adminUser;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void seed() {
        if (identityService.createGroupQuery().groupId(UNDERWRITERS).count() == 0) {
            Group group = identityService.newGroup(UNDERWRITERS);
            group.setName("Underwriters");
            group.setType("WORKFLOW");
            identityService.saveGroup(group);
            log.info("Created candidate group '{}'", UNDERWRITERS);
        }
        if (identityService.createUserQuery().userId(adminUser).memberOfGroup(UNDERWRITERS).count() == 0
                && identityService.createUserQuery().userId(adminUser).count() > 0) {
            identityService.createMembership(adminUser, UNDERWRITERS);
            log.info("Added '{}' to '{}'", adminUser, UNDERWRITERS);
        }
    }
}
