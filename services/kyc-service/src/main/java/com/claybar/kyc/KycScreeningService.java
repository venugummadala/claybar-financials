package com.claybar.kyc;

import org.springframework.stereotype.Service;

import java.text.Normalizer;
import java.time.Clock;
import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Locale;

/**
 * Rule-based KYC screening. Stateless and deterministic, so re-processing a redelivered command
 * yields the same answer (natural idempotency).
 */
@Service
public class KycScreeningService {

    private final KycProperties props;
    private final Clock clock;

    public KycScreeningService(KycProperties props, java.util.Optional<Clock> clock) {
        this.props = props;
        this.clock = clock.orElse(Clock.systemUTC());
    }

    public KycResult screen(String applicantName, String nationalId, String dateOfBirth) {
        if (isBlank(applicantName) || isBlank(nationalId)) {
            return new KycResult(KycResult.Status.FAILED, "Missing applicant name or national id");
        }
        String name = normalise(applicantName);
        if (matches(name, props.sanctionedNames())) {
            return new KycResult(KycResult.Status.FAILED, "Sanctions list match");
        }
        if (!isBlank(dateOfBirth)) {
            try {
                int age = Period.between(LocalDate.parse(dateOfBirth), LocalDate.now(clock)).getYears();
                if (age < props.minimumAge()) {
                    return new KycResult(KycResult.Status.FAILED, "Applicant under minimum age " + props.minimumAge());
                }
            } catch (DateTimeParseException e) {
                return new KycResult(KycResult.Status.FAILED, "Invalid date of birth");
            }
        }
        if (matches(name, props.politicallyExposedNames())) {
            return new KycResult(KycResult.Status.REFER, "Politically exposed person - enhanced due diligence required");
        }
        return new KycResult(KycResult.Status.VERIFIED, "Identity verified");
    }

    private static boolean matches(String normalisedName, List<String> list) {
        return list.stream().map(KycScreeningService::normalise).anyMatch(normalisedName::equals);
    }

    /** Case-, accent- and whitespace-insensitive comparison key. */
    static String normalise(String s) {
        return Normalizer.normalize(s, Normalizer.Form.NFD).replaceAll("\\p{M}", "")
                .trim().replaceAll("\\s+", " ").toLowerCase(Locale.ROOT);
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}
