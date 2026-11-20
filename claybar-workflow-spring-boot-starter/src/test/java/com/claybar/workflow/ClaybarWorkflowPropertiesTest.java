package com.claybar.workflow;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ClaybarWorkflowPropertiesTest {

    @Test
    void parsesCommaSeparatedTopics() {
        ClaybarWorkflowProperties props = new ClaybarWorkflowProperties();
        props.setInboundTopics(" a.events , b.events,, ");
        assertThat(props.inboundTopicArray()).containsExactly("a.events", "b.events");

        props.setInboundTopics("");
        assertThat(props.inboundTopicArray()).isEmpty();
    }
}
