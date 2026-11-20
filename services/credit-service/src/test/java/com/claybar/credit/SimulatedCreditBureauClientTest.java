package com.claybar.credit;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SimulatedCreditBureauClientTest {

    private final SimulatedCreditBureauClient client = new SimulatedCreditBureauClient();

    @Test
    void scoresAreDeterministicAndInRange() {
        for (String id : new String[]{"AB123456C", "QQ999999Z", "X"}) {
            int first = client.fetchReport(id, 50000, 10000).creditScore();
            assertThat(client.fetchReport(id, 50000, 10000).creditScore()).isEqualTo(first);
            assertThat(first).isBetween(300, 850);
        }
    }

    @Test
    void demoPrefixesProduceKnownBands() {
        assertThat(client.fetchReport("PRIME-1", 90000, 20000).riskBand()).isEqualTo(CreditReport.RiskBand.PRIME);
        assertThat(client.fetchReport("near-1", 90000, 20000).riskBand()).isEqualTo(CreditReport.RiskBand.NEAR_PRIME);
        assertThat(client.fetchReport("SUB-1", 90000, 20000).riskBand()).isEqualTo(CreditReport.RiskBand.SUBPRIME);
    }

    @Test
    void computesDebtToIncome() {
        assertThat(client.fetchReport("PRIME-1", 80000, 20000).debtToIncome()).isEqualTo(0.25);
        assertThat(client.fetchReport("PRIME-1", 0, 20000).debtToIncome()).isEqualTo(99d);
    }
}
