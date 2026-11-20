package com.claybar.kyc;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class KycScreeningServiceTest {

    private final KycScreeningService service = new KycScreeningService(
            new KycProperties(18, List.of("Ivan Sanctioned"), List.of("Pat Politician")),
            Optional.of(Clock.fixed(Instant.parse("2026-01-01T00:00:00Z"), ZoneOffset.UTC)));

    @Test
    void verifiesCleanAdult() {
        assertThat(service.screen("Ada Lovelace", "AB123", "1985-12-10").status()).isEqualTo(KycResult.Status.VERIFIED);
    }

    @Test
    void sanctionsMatchIsCaseAccentAndWhitespaceInsensitive() {
        KycResult result = service.screen("  ívan   SANCTIONED ", "AB123", "1980-01-01");
        assertThat(result.status()).isEqualTo(KycResult.Status.FAILED);
        assertThat(result.reason()).contains("Sanctions");
    }

    @Test
    void pepIsReferredNotFailed() {
        assertThat(service.screen("Pat Politician", "AB123", "1970-05-05").status()).isEqualTo(KycResult.Status.REFER);
    }

    @Test
    void minorsAndBadDataFail() {
        assertThat(service.screen("Young Person", "AB123", "2010-01-01").status()).isEqualTo(KycResult.Status.FAILED);
        assertThat(service.screen("Ada", "AB123", "not-a-date").status()).isEqualTo(KycResult.Status.FAILED);
        assertThat(service.screen("Ada", " ", "1985-01-01").status()).isEqualTo(KycResult.Status.FAILED);
    }
}
