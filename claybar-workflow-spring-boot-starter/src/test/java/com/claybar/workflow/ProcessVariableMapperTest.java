package com.claybar.workflow;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ProcessVariableMapperTest {

    private final ProcessVariableMapper mapper = new ProcessVariableMapper();

    @Test
    void keepsScalarsAndFlattensNestedStructuresToJson() {
        Map<String, Object> vars = mapper.toVariables(Map.of(
                "name", "Ada", "score", 720, "approved", true,
                "amount", new BigDecimal("25000.50"),
                "address", Map.of("city", "Leeds"),
                "tags", List.of("a", "b")));

        assertThat(vars).containsEntry("name", "Ada").containsEntry("score", 720)
                .containsEntry("approved", true).containsEntry("amount", 25000.50)
                .containsEntry("address", "{\"city\":\"Leeds\"}")
                .containsEntry("tags", "[\"a\",\"b\"]");
    }

    @Test
    void selectsNamedVariablesOnly() {
        Map<String, Object> vars = Map.of("a", 1, "b", 2, "c", 3);
        assertThat(mapper.select(vars, "a, c, missing")).containsOnlyKeys("a", "c");
        assertThat(mapper.select(vars, "*")).containsOnlyKeys("a", "b", "c");
        assertThat(mapper.select(vars, null)).isEmpty();
    }
}
