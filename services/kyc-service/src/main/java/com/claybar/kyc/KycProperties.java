package com.claybar.kyc;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@ConfigurationProperties(prefix = "claybar.kyc")
public record KycProperties(int minimumAge, List<String> sanctionedNames, List<String> politicallyExposedNames) {

    public KycProperties {
        minimumAge = minimumAge <= 0 ? 18 : minimumAge;
        sanctionedNames = sanctionedNames == null ? List.of() : List.copyOf(sanctionedNames);
        politicallyExposedNames = politicallyExposedNames == null ? List.of() : List.copyOf(politicallyExposedNames);
    }
}
