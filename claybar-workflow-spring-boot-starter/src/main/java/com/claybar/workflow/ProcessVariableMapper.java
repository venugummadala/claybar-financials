package com.claybar.workflow;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Converts event payloads to process variables and back.
 *
 * <p>Only scalar values become typed process variables. Nested structures are stored as JSON
 * strings: this keeps the engine free of Java-serialised objects (a classic Camunda production
 * pitfall — class evolution breaks running instances and it is a deserialisation attack vector).
 */
public class ProcessVariableMapper {

    private final ObjectMapper json = new ObjectMapper();

    public Map<String, Object> toVariables(Map<String, Object> payload) {
        Map<String, Object> vars = new HashMap<>();
        payload.forEach((key, value) -> vars.put(key, toVariable(value)));
        return vars;
    }

    Object toVariable(Object value) {
        if (value == null || value instanceof String || value instanceof Boolean
                || value instanceof Integer || value instanceof Long || value instanceof Double) {
            return value;
        }
        if (value instanceof BigDecimal bd) {
            return bd.doubleValue();
        }
        if (value instanceof BigInteger bi) {
            return bi.longValue();
        }
        if (value instanceof Number n) {
            return n.doubleValue();
        }
        if (value instanceof Map<?, ?> || value instanceof Collection<?>) {
            try {
                return json.writeValueAsString(value);
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException("Cannot convert payload value to JSON", e);
            }
        }
        return value.toString();
    }

    /** Selects variables by comma-separated names, or all variables for {@code "*"}. */
    public Map<String, Object> select(Map<String, Object> variables, String names) {
        if (names == null || names.isBlank()) {
            return Map.of();
        }
        if ("*".equals(names.trim())) {
            Map<String, Object> all = new LinkedHashMap<>();
            variables.forEach((k, v) -> all.put(k, toVariable(v)));
            return all;
        }
        Map<String, Object> selected = new LinkedHashMap<>();
        for (String raw : names.split(",")) {
            String name = raw.trim();
            if (!name.isEmpty() && variables.containsKey(name)) {
                selected.put(name, toVariable(variables.get(name)));
            }
        }
        return selected;
    }
}
