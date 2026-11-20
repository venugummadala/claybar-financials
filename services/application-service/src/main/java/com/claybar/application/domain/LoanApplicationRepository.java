package com.claybar.application.domain;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface LoanApplicationRepository extends JpaRepository<LoanApplication, String> {

    Optional<LoanApplication> findByIdempotencyKey(String idempotencyKey);
}
