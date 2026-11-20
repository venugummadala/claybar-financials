package com.claybar.application;

import com.claybar.application.domain.LoanApplicationRepository;
import com.claybar.application.domain.LoanStatus;
import com.claybar.application.messaging.LoanDecisionProjector;
import com.claybar.application.messaging.ProcessedEventRepository;
import com.claybar.application.outbox.OutboxRelay;
import com.claybar.application.outbox.OutboxRepository;
import com.claybar.events.EventCodec;
import com.claybar.events.EventEnvelope;
import com.claybar.events.EventPublisher;
import com.claybar.events.EventTypes;
import com.claybar.events.Topics;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = {
        "spring.kafka.listener.auto-startup=false",
        "claybar.kafka.create-topics=false",
        "claybar.outbox.relay.enabled=false",
})
@AutoConfigureMockMvc
class LoanApplicationApiTest {

    private static final String VALID = """
            {"applicantName":"Ada Lovelace","nationalId":"PRIME-1","email":"ada@example.com",
             "dateOfBirth":"1985-12-10","loanAmount":20000,"termMonths":36,"annualIncome":90000}
            """;

    @Autowired MockMvc mvc;
    @Autowired ObjectMapper json;
    @Autowired EventCodec codec;
    @Autowired OutboxRepository outbox;
    @Autowired OutboxRelay relay;
    @Autowired LoanApplicationRepository applications;
    @Autowired ProcessedEventRepository processed;
    @Autowired LoanDecisionProjector projector;

    @MockBean EventPublisher publisher;

    @BeforeEach
    void clean() {
        outbox.deleteAll();
        processed.deleteAll();
        applications.deleteAll();
    }

    @Test
    void submitPersistsApplicationAndOutboxEventAtomically() throws Exception {
        String id = submit(VALID, null);

        var rows = outbox.findAll();
        assertThat(rows).hasSize(1);
        EventEnvelope event = codec.decode(rows.get(0).getEnvelope());
        assertThat(event.eventType()).isEqualTo(EventTypes.LOAN_APPLICATION_SUBMITTED);
        assertThat(event.correlationId()).isEqualTo(id);
        assertThat(rows.get(0).getTopic()).isEqualTo(Topics.LOAN_APPLICATIONS);
        assertThat(event.payload()).containsEntry("nationalId", "PRIME-1").containsKey("annualIncome");
    }

    @Test
    void idempotencyKeyMakesRetriesSafe() throws Exception {
        String first = submit(VALID, "client-key-1");
        String second = submit(VALID, "client-key-1");

        assertThat(second).isEqualTo(first);
        assertThat(applications.count()).isOne();
        assertThat(outbox.count()).isOne();
    }

    @Test
    void invalidRequestReturnsProblemDetailWithFieldErrors() throws Exception {
        mvc.perform(post("/api/v1/loan-applications").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"applicantName\":\"\",\"email\":\"nope\",\"loanAmount\":5,\"termMonths\":1}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.title").value("Bad Request"))
                .andExpect(jsonPath("$.errors.email").exists())
                .andExpect(jsonPath("$.errors.loanAmount").exists())
                .andExpect(jsonPath("$.errors.termMonths").exists());
    }

    @Test
    void unknownApplicationIs404() throws Exception {
        mvc.perform(get("/api/v1/loan-applications/APP-NOPE")).andExpect(status().isNotFound());
    }

    @Test
    void decisionsAreProjectedIdempotentlyAndFinalStatesAreSticky() throws Exception {
        String id = submit(VALID, null);
        EventEnvelope approved = EventEnvelope.of(EventTypes.LOAN_APPROVED, id, "orchestrator",
                Map.of("riskDecision", "APPROVE"));

        // Through the Kafka entry point (as in production), then a redelivery of the same record.
        var record = new org.apache.kafka.clients.consumer.ConsumerRecord<>(Topics.LOAN_DECISIONS, 0, 0L, id, codec.encode(approved));
        projector.onDecision(record);
        projector.onDecision(record);
        projector.apply(EventEnvelope.of(EventTypes.LOAN_UNDER_REVIEW, id, "orchestrator", Map.of())); // late / out of order

        mvc.perform(get("/api/v1/loan-applications/" + id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value(LoanStatus.APPROVED.name()))
                .andExpect(jsonPath("$.decisionReason").value("Risk decision: APPROVE"))
                .andExpect(jsonPath("$.nationalId").doesNotExist());
        assertThat(processed.count()).isEqualTo(2);
    }

    @Test
    void relayPublishesInOrderAndStopsAtFirstFailure() throws Exception {
        submit(VALID, "k1");
        submit(VALID, "k2");

        doThrow(new IllegalStateException("broker down")).when(publisher).publishAndWait(any(), any(), any());
        assertThat(relay.relayBatch()).isZero();
        verify(publisher, times(1)).publishAndWait(eq(Topics.LOAN_APPLICATIONS), any(), any());
        assertThat(outbox.countByPublishedAtIsNull()).isEqualTo(2);

        org.mockito.Mockito.reset(publisher);
        assertThat(relay.relayBatch()).isEqualTo(2);
        assertThat(outbox.countByPublishedAtIsNull()).isZero();
    }

    private String submit(String body, String idempotencyKey) throws Exception {
        var request = post("/api/v1/loan-applications").contentType(MediaType.APPLICATION_JSON).content(body);
        if (idempotencyKey != null) {
            request.header("Idempotency-Key", idempotencyKey);
        }
        var result = mvc.perform(request)
                .andExpect(status().isCreated())
                .andExpect(header().exists("Location"))
                .andExpect(jsonPath("$.status").value("SUBMITTED"))
                .andReturn();
        JsonNode node = json.readTree(result.getResponse().getContentAsString());
        return node.get("id").asText();
    }
}
