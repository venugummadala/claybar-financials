package com.claybar.orchestrator;

import org.camunda.bpm.dmn.engine.DmnDecision;
import org.camunda.bpm.dmn.engine.DmnEngine;
import org.camunda.bpm.dmn.engine.DmnEngineConfiguration;
import org.camunda.bpm.engine.variable.Variables;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.InputStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Pure DMN unit test: no Spring, no engine database — runs in milliseconds. */
class LoanRiskDecisionTest {

    private static DmnEngine dmnEngine;
    private static DmnDecision decision;

    @BeforeAll
    static void parse() throws Exception {
        dmnEngine = DmnEngineConfiguration.createDefaultDmnEngineConfiguration().buildEngine();
        try (InputStream in = LoanRiskDecisionTest.class.getResourceAsStream("/bpmn/loan-risk-assessment.dmn")) {
            decision = dmnEngine.parseDecision("loan-risk-assessment", in);
        }
    }

    @ParameterizedTest(name = "kyc={0} credit={1} score={2} amount={3} income={4} -> {5}")
    @CsvSource({
            "VERIFIED, COMPLETED, 780, 20000, 90000, APPROVE",
            "VERIFIED, COMPLETED, 700, 36000, 90000, APPROVE",   // ratio 0.4 boundary
            "VERIFIED, COMPLETED, 780, 60000, 200000, REVIEW",   // above auto-approval limit
            "VERIFIED, COMPLETED, 650, 20000, 90000, REVIEW",    // near-prime
            "VERIFIED, COMPLETED, 549, 20000, 90000, REJECT",    // sub-prime
            "VERIFIED, COMPLETED, 780, 95000, 90000, REJECT",    // loan > income
            "FAILED,   COMPLETED, 820, 10000, 90000, REJECT",    // KYC failure beats great score
            "REFER,    COMPLETED, 780, 20000, 90000, REVIEW",    // PEP / adverse media
            "TIMEOUT,  COMPLETED, 780, 20000, 90000, REVIEW",
            "VERIFIED, TIMEOUT,   0,   20000, 90000, REVIEW",    // timeout beats score-0 reject
            "VERIFIED, COMPLETED, 780, 20000, 0,     REJECT",    // no income
    })
    void decides(String kyc, String credit, int score, double amount, double income, String expected) {
        var result = dmnEngine.evaluateDecisionTable(decision, Variables.createVariables()
                .putValue("kycStatus", kyc)
                .putValue("creditStatus", credit)
                .putValue("creditScore", score)
                .putValue("loanAmount", amount)
                .putValue("annualIncome", income));

        assertThat((String) result.getSingleEntry()).isEqualTo(expected);
    }
}
