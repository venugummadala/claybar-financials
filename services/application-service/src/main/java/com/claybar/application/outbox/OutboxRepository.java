package com.claybar.application.outbox;

import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;

import java.time.Instant;
import java.util.List;

public interface OutboxRepository extends JpaRepository<OutboxEvent, String> {

    /**
     * Claims a batch of pending rows. {@code SELECT ... FOR UPDATE SKIP LOCKED} (lock timeout -2)
     * lets several application-service replicas relay concurrently without double-publishing.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints(@QueryHint(name = "jakarta.persistence.lock.timeout", value = "-2"))
    @Query("select o from OutboxEvent o where o.publishedAt is null order by o.createdAt")
    List<OutboxEvent> claimPending(Pageable page);

    long countByPublishedAtIsNull();

    @Modifying
    @Query("delete from OutboxEvent o where o.publishedAt is not null and o.publishedAt < :cutoff")
    int deletePublishedBefore(Instant cutoff);
}
