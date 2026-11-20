package com.claybar.credit;

import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.zip.CRC32;

/**
 * Deterministic bureau simulator. The score is derived from the national id so the same applicant
 * always scores the same (replays are idempotent). Prefixes make demo scenarios scriptable:
 * {@code PRIME-*} -> 800, {@code NEAR-*} -> 650, {@code SUB-*} -> 480.
 */
@Component
public class SimulatedCreditBureauClient implements CreditBureauClient {

    @Override
    public CreditReport fetchReport(String nationalId, double annualIncome, double requestedAmount) {
        int score = scoreFor(nationalId);
        double dti = annualIncome > 0 ? Math.round(requestedAmount / annualIncome * 1000d) / 1000d : 99d;
        String reference = "BUR-" + UUID.nameUUIDFromBytes(nationalId.getBytes(StandardCharsets.UTF_8))
                .toString().substring(0, 8).toUpperCase();
        return new CreditReport(score, CreditReport.band(score), dti, reference);
    }

    static int scoreFor(String nationalId) {
        String id = nationalId.toUpperCase();
        if (id.startsWith("PRIME-")) return 800;
        if (id.startsWith("NEAR-")) return 650;
        if (id.startsWith("SUB-")) return 480;
        CRC32 crc = new CRC32();
        crc.update(id.getBytes(StandardCharsets.UTF_8));
        return 300 + (int) (crc.getValue() % 551); // FICO-like range 300..850
    }
}
