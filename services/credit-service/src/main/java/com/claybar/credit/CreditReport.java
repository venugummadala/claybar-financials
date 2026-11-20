package com.claybar.credit;

public record CreditReport(int creditScore, RiskBand riskBand, double debtToIncome, String bureauReference) {

    public enum RiskBand { PRIME, NEAR_PRIME, SUBPRIME }

    static RiskBand band(int score) {
        if (score >= 700) {
            return RiskBand.PRIME;
        }
        return score >= 600 ? RiskBand.NEAR_PRIME : RiskBand.SUBPRIME;
    }
}
