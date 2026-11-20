# BPMN snippets (Camunda 7, claybar-workflow plug-in)

Namespaces assumed on `<bpmn:definitions>`: `bpmn`, `bpmndi`, `dc`, `di`, `camunda="http://camunda.org/schema/1.0/bpmn"`,
`xsi`. Replace `Fraud`/`fraud` with your step's name. Element order inside a flow node: `extensionElements`,
`incoming`, `outgoing`, then event definitions.

## Contents
1. Message definitions
2. Request/reply block with timeout
3. Parallel branch wiring
4. DMN business rule task + gateway
5. User task with form and SLA escalation
6. Publishing an outcome
7. Message start event
8. Diagram interchange (DI)

## 1. Message definitions (direct children of `definitions`)

```xml
<bpmn:message id="Msg_FraudCheckCompleted" name="FraudCheckCompleted" />
```
`name` must equal the reply's `eventType`.

## 2. Request/reply block with timeout

```xml
<bpmn:sendTask id="Task_RequestFraudCheck" name="Request fraud check"
               camunda:asyncBefore="true" camunda:delegateExpression="${kafkaCommandDelegate}">
  <bpmn:extensionElements>
    <camunda:inputOutput>
      <camunda:inputParameter name="topic">claybar.fraud.commands</camunda:inputParameter>
      <camunda:inputParameter name="eventType">FraudCheckRequested</camunda:inputParameter>
      <camunda:inputParameter name="payloadVariables">applicantName,nationalId,loanAmount</camunda:inputParameter>
    </camunda:inputOutput>
  </bpmn:extensionElements>
  <bpmn:incoming>Flow_ForkFraud</bpmn:incoming>
  <bpmn:outgoing>Flow_FraudSent</bpmn:outgoing>
</bpmn:sendTask>

<bpmn:receiveTask id="Task_AwaitFraudCheck" name="Await fraud result"
                  messageRef="Msg_FraudCheckCompleted" camunda:asyncAfter="true">
  <bpmn:incoming>Flow_FraudSent</bpmn:incoming>
  <bpmn:outgoing>Flow_FraudReceived</bpmn:outgoing>
</bpmn:receiveTask>

<bpmn:boundaryEvent id="Timer_FraudTimeout" name="Fraud SLA exceeded"
                    attachedToRef="Task_AwaitFraudCheck" cancelActivity="true">
  <bpmn:outgoing>Flow_FraudTimeout</bpmn:outgoing>
  <bpmn:timerEventDefinition id="Ted_Fraud">
    <bpmn:timeDuration xsi:type="bpmn:tFormalExpression">${loanSla.fraudTimeout}</bpmn:timeDuration>
  </bpmn:timerEventDefinition>
</bpmn:boundaryEvent>

<bpmn:serviceTask id="Task_FraudTimedOut" name="Mark fraud check timed out" camunda:expression="${true}">
  <bpmn:extensionElements>
    <camunda:inputOutput>
      <camunda:outputParameter name="fraudStatus">TIMEOUT</camunda:outputParameter>
      <camunda:outputParameter name="fraudScore">${0}</camunda:outputParameter>
    </camunda:inputOutput>
  </bpmn:extensionElements>
  <bpmn:incoming>Flow_FraudTimeout</bpmn:incoming>
  <bpmn:outgoing>Flow_FraudTimedOut</bpmn:outgoing>
</bpmn:serviceTask>

<bpmn:exclusiveGateway id="Gateway_FraudMerge">
  <bpmn:incoming>Flow_FraudReceived</bpmn:incoming>
  <bpmn:incoming>Flow_FraudTimedOut</bpmn:incoming>
  <bpmn:outgoing>Flow_FraudDone</bpmn:outgoing>
</bpmn:exclusiveGateway>
```

`${0}` (not `0`) makes the output a number; plain text is a String.

SLA bean property (Java), referenced as `${loanSla.fraudTimeout}`:

```java
private Duration fraudTimeout = Duration.ofMinutes(10);
public String getFraudTimeout() { return fraudTimeout.toString(); }   // ISO-8601 for the timer
public void setFraudTimeout(Duration d) { this.fraudTimeout = d; }
```

## 3. Parallel branch wiring

Add an outgoing flow on the existing fork and an incoming flow on the existing join:

```xml
<bpmn:sequenceFlow id="Flow_ForkFraud" sourceRef="Gateway_Fork" targetRef="Task_RequestFraudCheck" />
<bpmn:sequenceFlow id="Flow_FraudSent" sourceRef="Task_RequestFraudCheck" targetRef="Task_AwaitFraudCheck" />
<bpmn:sequenceFlow id="Flow_FraudReceived" sourceRef="Task_AwaitFraudCheck" targetRef="Gateway_FraudMerge" />
<bpmn:sequenceFlow id="Flow_FraudTimeout" sourceRef="Timer_FraudTimeout" targetRef="Task_FraudTimedOut" />
<bpmn:sequenceFlow id="Flow_FraudTimedOut" sourceRef="Task_FraudTimedOut" targetRef="Gateway_FraudMerge" />
<bpmn:sequenceFlow id="Flow_FraudDone" sourceRef="Gateway_FraudMerge" targetRef="Gateway_Join" />
```
…and add `<bpmn:outgoing>Flow_ForkFraud</bpmn:outgoing>` to the fork and `<bpmn:incoming>Flow_FraudDone</bpmn:incoming>`
to the join.

## 4. DMN business rule task + gateway

```xml
<bpmn:businessRuleTask id="Task_AssessRisk" name="Assess risk"
    camunda:decisionRef="loan-risk-assessment" camunda:resultVariable="riskDecision"
    camunda:mapDecisionResult="singleEntry">…</bpmn:businessRuleTask>

<bpmn:sequenceFlow id="Flow_Reject" name="Reject" sourceRef="Gateway_RiskDecision" targetRef="Task_PublishRejected">
  <bpmn:conditionExpression xsi:type="bpmn:tFormalExpression">${riskDecision == 'REJECT'}</bpmn:conditionExpression>
</bpmn:sequenceFlow>
```

DMN input (FEEL) and rule row:

```xml
<input id="Input_FraudScore" label="Fraud score" camunda:inputVariable="input">
  <inputExpression id="Input_FraudScore_expr" typeRef="integer"><text>fraudScore</text></inputExpression>
</input>
…
<rule id="Rule_HighFraud">
  <description>High fraud score</description>
  <inputEntry id="R_HighFraud_in0"><text>&gt;= 80</text></inputEntry>   <!-- one inputEntry per input, "-" = any -->
  <outputEntry id="R_HighFraud_out"><text>"REJECT"</text></outputEntry>
</rule>
```
Adding an input column means adding an `inputEntry` (usually `-`) to **every existing rule**, in column order.
Decisions need `camunda:historyTimeToLive`.

## 5. User task with form and SLA escalation

```xml
<bpmn:userTask id="UserTask_Underwrite" name="Manual underwriting"
    camunda:candidateGroups="underwriters" camunda:dueDate="${loanSla.underwritingDueDate()}">
  <bpmn:extensionElements>
    <camunda:formData>
      <camunda:formField id="underwritingApproved" label="Approve loan?" type="boolean">
        <camunda:validation><camunda:constraint name="required" /></camunda:validation>
      </camunda:formField>
      <camunda:formField id="underwriterComment" label="Comment" type="string" />
    </camunda:formData>
  </bpmn:extensionElements>
  …
</bpmn:userTask>
<bpmn:boundaryEvent id="Timer_UnderwritingSla" attachedToRef="UserTask_Underwrite" cancelActivity="false">
  <bpmn:outgoing>Flow_SlaBreached</bpmn:outgoing>
  <bpmn:timerEventDefinition id="Ted_Uw">
    <bpmn:timeDuration xsi:type="bpmn:tFormalExpression">${loanSla.underwritingSla}</bpmn:timeDuration>
  </bpmn:timerEventDefinition>
</bpmn:boundaryEvent>
```
The escalation path is a send task (event to ops) → end event.

## 6. Publishing an outcome

Same as the send task in §2 with a decisions topic and no receive task after it, then `<bpmn:endEvent>`.

## 7. Message start event

```xml
<bpmn:startEvent id="Start_PaymentInitiated" name="Payment initiated">
  <bpmn:outgoing>Flow_Start</bpmn:outgoing>
  <bpmn:messageEventDefinition id="Med_Start" messageRef="Msg_PaymentInitiated" />
</bpmn:startEvent>
```
Plus `claybar.workflow.start-events: PaymentInitiated` and the topic in `inbound-topics`. Process element:
`<bpmn:process id="payment" isExecutable="true" camunda:historyTimeToLive="P180D">`.

## 8. Diagram interchange (DI)

Inside `<bpmndi:BPMNPlane bpmnElement="<process id>">`:

```xml
<bpmndi:BPMNShape id="Task_RequestFraudCheck_di" bpmnElement="Task_RequestFraudCheck">
  <dc:Bounds x="340" y="620" width="100" height="80" />
</bpmndi:BPMNShape>
<bpmndi:BPMNShape id="Gateway_FraudMerge_di" bpmnElement="Gateway_FraudMerge" isMarkerVisible="true">
  <dc:Bounds x="755" y="635" width="50" height="50" />
</bpmndi:BPMNShape>
<bpmndi:BPMNEdge id="Flow_FraudSent_di" bpmnElement="Flow_FraudSent">
  <di:waypoint x="440" y="660" />
  <di:waypoint x="500" y="660" />
</bpmndi:BPMNEdge>
```
Boundary event shapes sit on the edge of the task they are attached to (centre on the task's top or bottom border).
