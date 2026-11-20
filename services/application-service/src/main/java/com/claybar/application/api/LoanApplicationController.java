package com.claybar.application.api;

import com.claybar.application.domain.LoanApplicationService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Size;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

@RestController
@Validated
@RequestMapping("/api/v1/loan-applications")
@Tag(name = "Loan applications")
public class LoanApplicationController {

    private final LoanApplicationService service;

    public LoanApplicationController(LoanApplicationService service) {
        this.service = service;
    }

    @PostMapping
    @Operation(summary = "Submit a loan application",
            description = "Persists the application and starts the loan-origination workflow asynchronously. "
                    + "Poll GET /{id} (or consume claybar.loan.decisions) for the decision.")
    public ResponseEntity<LoanApplicationResponse> submit(
            @Valid @RequestBody LoanApplicationRequest request,
            @Parameter(description = "Client-generated key; retries with the same key return the original application")
            @RequestHeader(name = "Idempotency-Key", required = false) @Size(max = 100) String idempotencyKey) {
        var application = service.submit(request.toCommand(), idempotencyKey);
        var location = ServletUriComponentsBuilder.fromCurrentRequest().path("/{id}").buildAndExpand(application.getId()).toUri();
        return ResponseEntity.created(location).body(LoanApplicationResponse.from(application));
    }

    @GetMapping("/{id}")
    @Operation(summary = "Get an application and its current status")
    public LoanApplicationResponse get(@PathVariable String id) {
        return service.find(id).map(LoanApplicationResponse::from)
                .orElseThrow(() -> new ApplicationNotFoundException(id));
    }
}
