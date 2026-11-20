# Regenerates loan-origination.bpmn (model + diagram layout). Usage: python3 scripts/generate-loan-origination-bpmn.py <out.bpmn>
import sys
out = sys.argv[1]
SIZE = {"task": (100, 80), "event": (36, 36), "gateway": (50, 50)}
nodes = {}   # id -> dict(kind, tag, cx, cy, xml_attrs, inner, name)
order = []
def node(id, tag, kind, cx, cy, name=None, attrs="", inner="", attached=None):
    nodes[id] = dict(tag=tag, kind=kind, cx=cx, cy=cy, name=name, attrs=attrs, inner=inner, attached=attached, inc=[], out=[])
    order.append(id)
flows = []
def flow(id, s, t, name=None, cond=None, vfirst=None):
    flows.append(dict(id=id, s=s, t=t, name=name, cond=cond, vfirst=vfirst))
    nodes[s]["out"].append(id); nodes[t]["inc"].append(id)

def io(params, outputs=()):
    p = "".join(f'<camunda:inputParameter name="{k}">{v}</camunda:inputParameter>' for k, v in params)
    o = "".join(f'<camunda:outputParameter name="{k}">{v}</camunda:outputParameter>' for k, v in outputs)
    return f"<bpmn:extensionElements><camunda:inputOutput>{p}{o}</camunda:inputOutput></bpmn:extensionElements>"

SEND = 'camunda:asyncBefore="true" camunda:delegateExpression="${kafkaCommandDelegate}"'
DEC = "claybar.loan.decisions"
KYC, MAIN, CREDIT, REJ = 200, 360, 520, 620

node("Start_LoanSubmitted", "startEvent", "event", 168, MAIN, "Loan application submitted",
     inner='<bpmn:messageEventDefinition id="Med_Start" messageRef="Msg_LoanApplicationSubmitted" />')
node("Gateway_Fork", "parallelGateway", "gateway", 265, MAIN)
node("Task_RequestKyc", "sendTask", "task", 390, KYC, "Request KYC check", SEND,
     io([("topic", "claybar.kyc.commands"), ("eventType", "KycRequested"),
         ("payloadVariables", "applicantName,nationalId,email,dateOfBirth")]))
node("Task_AwaitKyc", "receiveTask", "task", 550, KYC, "Await KYC result",
     'messageRef="Msg_KycCompleted" camunda:asyncAfter="true"')
node("Timer_KycTimeout", "boundaryEvent", "event", 550, KYC - 40, "KYC SLA exceeded",
     'cancelActivity="true"', '<bpmn:timerEventDefinition id="Ted_Kyc"><bpmn:timeDuration xsi:type="bpmn:tFormalExpression">${loanSla.kycTimeout}</bpmn:timeDuration></bpmn:timerEventDefinition>',
     attached="Task_AwaitKyc")
node("Task_KycTimedOut", "serviceTask", "task", 670, KYC - 110, "Mark KYC timed out", 'camunda:expression="${true}"',
     io([], [("kycStatus", "TIMEOUT"), ("kycReason", "No KYC response within SLA")]))
node("Gateway_KycMerge", "exclusiveGateway", "gateway", 780, KYC)
node("Task_RequestCredit", "sendTask", "task", 390, CREDIT, "Request credit check", SEND,
     io([("topic", "claybar.credit.commands"), ("eventType", "CreditCheckRequested"),
         ("payloadVariables", "applicantName,nationalId,annualIncome,loanAmount,termMonths")]))
node("Task_AwaitCredit", "receiveTask", "task", 550, CREDIT, "Await credit result",
     'messageRef="Msg_CreditCheckCompleted" camunda:asyncAfter="true"')
node("Timer_CreditTimeout", "boundaryEvent", "event", 550, CREDIT + 40, "Bureau SLA exceeded",
     'cancelActivity="true"', '<bpmn:timerEventDefinition id="Ted_Credit"><bpmn:timeDuration xsi:type="bpmn:tFormalExpression">${loanSla.creditTimeout}</bpmn:timeDuration></bpmn:timerEventDefinition>',
     attached="Task_AwaitCredit")
node("Task_CreditTimedOut", "serviceTask", "task", 670, CREDIT + 110, "Mark credit check timed out", 'camunda:expression="${true}"',
     io([], [("creditStatus", "TIMEOUT"), ("creditScore", "${0}")]))
node("Gateway_CreditMerge", "exclusiveGateway", "gateway", 780, CREDIT)
node("Gateway_Join", "parallelGateway", "gateway", 880, MAIN)
node("Task_AssessRisk", "businessRuleTask", "task", 1000, MAIN, "Assess loan risk",
     'camunda:decisionRef="loan-risk-assessment" camunda:resultVariable="riskDecision" camunda:mapDecisionResult="singleEntry"')
node("Gateway_RiskDecision", "exclusiveGateway", "gateway", 1130, MAIN, "Risk decision?")
node("Task_NotifyUnderReview", "sendTask", "task", 1250, MAIN, "Publish loan under review", SEND,
     io([("topic", DEC), ("eventType", "LoanUnderReview"), ("payloadVariables", "riskDecision,creditScore,kycStatus")]))
node("UserTask_Underwrite", "userTask", "task", 1390, MAIN, "Manual underwriting",
     'camunda:candidateGroups="underwriters" camunda:dueDate="${loanSla.underwritingDueDate()}"',
     '<bpmn:extensionElements><camunda:formData>'
     '<camunda:formField id="underwritingApproved" label="Approve loan?" type="boolean"><camunda:validation><camunda:constraint name="required" /></camunda:validation></camunda:formField>'
     '<camunda:formField id="underwriterComment" label="Comment" type="string" />'
     '</camunda:formData></bpmn:extensionElements>')
node("Timer_UnderwritingSla", "boundaryEvent", "event", 1390, MAIN + 40, "Underwriting SLA breached",
     'cancelActivity="false"', '<bpmn:timerEventDefinition id="Ted_Uw"><bpmn:timeDuration xsi:type="bpmn:tFormalExpression">${loanSla.underwritingSla}</bpmn:timeDuration></bpmn:timerEventDefinition>',
     attached="UserTask_Underwrite")
node("Task_EscalateSla", "sendTask", "task", 1500, MAIN + 130, "Publish SLA breach", SEND,
     io([("topic", DEC), ("eventType", "UnderwritingSlaBreached"), ("payloadVariables", "applicantName,loanAmount")]))
node("End_Escalated", "endEvent", "event", 1640, MAIN + 130, "Escalated")
node("Gateway_UwOutcome", "exclusiveGateway", "gateway", 1520, MAIN, "Underwriter approved?")
node("Task_PublishApproved", "sendTask", "task", 1680, KYC, "Publish loan approved", SEND,
     io([("topic", DEC), ("eventType", "LoanApproved"),
         ("payloadVariables", "applicantName,email,loanAmount,termMonths,creditScore,riskDecision,underwriterComment")]))
node("End_Approved", "endEvent", "event", 1820, KYC, "Loan approved")
node("Task_PublishRejected", "sendTask", "task", 1680, REJ + 60, "Publish loan rejected", SEND,
     io([("topic", DEC), ("eventType", "LoanRejected"),
         ("payloadVariables", "applicantName,email,loanAmount,creditScore,kycStatus,kycReason,riskDecision,underwriterComment")]))
node("End_Rejected", "endEvent", "event", 1820, REJ + 60, "Loan rejected")

flow("Flow_Start", "Start_LoanSubmitted", "Gateway_Fork")
flow("Flow_ForkKyc", "Gateway_Fork", "Task_RequestKyc")
flow("Flow_ForkCredit", "Gateway_Fork", "Task_RequestCredit")
flow("Flow_KycSent", "Task_RequestKyc", "Task_AwaitKyc")
flow("Flow_KycReceived", "Task_AwaitKyc", "Gateway_KycMerge")
flow("Flow_KycTimeout", "Timer_KycTimeout", "Task_KycTimedOut", vfirst=True)
flow("Flow_KycTimedOut", "Task_KycTimedOut", "Gateway_KycMerge")
flow("Flow_KycDone", "Gateway_KycMerge", "Gateway_Join")
flow("Flow_CreditSent", "Task_RequestCredit", "Task_AwaitCredit")
flow("Flow_CreditReceived", "Task_AwaitCredit", "Gateway_CreditMerge")
flow("Flow_CreditTimeout", "Timer_CreditTimeout", "Task_CreditTimedOut", vfirst=True)
flow("Flow_CreditTimedOut", "Task_CreditTimedOut", "Gateway_CreditMerge")
flow("Flow_CreditDone", "Gateway_CreditMerge", "Gateway_Join")
flow("Flow_Joined", "Gateway_Join", "Task_AssessRisk")
flow("Flow_Assessed", "Task_AssessRisk", "Gateway_RiskDecision")
flow("Flow_AutoApprove", "Gateway_RiskDecision", "Task_PublishApproved", "Approve", "${riskDecision == 'APPROVE'}")
flow("Flow_Review", "Gateway_RiskDecision", "Task_NotifyUnderReview", "Review", "${riskDecision == 'REVIEW'}")
flow("Flow_AutoReject", "Gateway_RiskDecision", "Task_PublishRejected", "Reject", "${riskDecision == 'REJECT'}")
flow("Flow_ToUnderwriting", "Task_NotifyUnderReview", "UserTask_Underwrite")
flow("Flow_Underwritten", "UserTask_Underwrite", "Gateway_UwOutcome")
flow("Flow_UwApproved", "Gateway_UwOutcome", "Task_PublishApproved", "Yes", "${underwritingApproved}")
flow("Flow_UwRejected", "Gateway_UwOutcome", "Task_PublishRejected", "No", "${!underwritingApproved}")
flow("Flow_SlaBreached", "Timer_UnderwritingSla", "Task_EscalateSla", vfirst=True)
flow("Flow_Escalated", "Task_EscalateSla", "End_Escalated")
flow("Flow_Approved", "Task_PublishApproved", "End_Approved")
flow("Flow_Rejected", "Task_PublishRejected", "End_Rejected")

def esc(s): return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")
def bounds(n):
    w, h = SIZE[n["kind"]]; return n["cx"] - w / 2, n["cy"] - h / 2, w, h

proc = []
for i in order:
    n = nodes[i]
    nm = f' name="{esc(n["name"])}"' if n["name"] else ""
    att = f' attachedToRef="{n["attached"]}"' if n["attached"] else ""
    body = n["inner"] + "".join(f"<bpmn:incoming>{f}</bpmn:incoming>" for f in n["inc"]) + "".join(f"<bpmn:outgoing>{f}</bpmn:outgoing>" for f in n["out"])
    # extensionElements must come first; timer/message definitions after incoming/outgoing
    if n["inner"].startswith("<bpmn:extensionElements"):
        body = n["inner"] + "".join(f"<bpmn:incoming>{f}</bpmn:incoming>" for f in n["inc"]) + "".join(f"<bpmn:outgoing>{f}</bpmn:outgoing>" for f in n["out"])
    elif n["inner"]:
        body = "".join(f"<bpmn:incoming>{f}</bpmn:incoming>" for f in n["inc"]) + "".join(f"<bpmn:outgoing>{f}</bpmn:outgoing>" for f in n["out"]) + n["inner"]
    proc.append(f'    <bpmn:{n["tag"]} id="{i}"{nm}{att} {n["attrs"]}>{body}</bpmn:{n["tag"]}>'.replace(" >", ">"))
for f in flows:
    nm = f' name="{esc(f["name"])}"' if f["name"] else ""
    c = f'<bpmn:conditionExpression xsi:type="bpmn:tFormalExpression">{esc(f["cond"])}</bpmn:conditionExpression>' if f["cond"] else ""
    proc.append(f'    <bpmn:sequenceFlow id="{f["id"]}"{nm} sourceRef="{f["s"]}" targetRef="{f["t"]}">{c}</bpmn:sequenceFlow>' if c else
                f'    <bpmn:sequenceFlow id="{f["id"]}"{nm} sourceRef="{f["s"]}" targetRef="{f["t"]}" />')

di = []
for i in order:
    n = nodes[i]; x, y, w, h = bounds(n)
    extra = ' isMarkerVisible="true"' if n["tag"] == "exclusiveGateway" else ""
    lbl = ""
    if n["kind"] != "task" and n["name"]:
        top = n["attached"] and n["cy"] < nodes[n["attached"]]["cy"]
        ly = y - 31 if top else y + h + 4
        lx = x + w + 4 if top else x - 30
        lbl = f'<bpmndi:BPMNLabel><dc:Bounds x="{lx:.0f}" y="{ly:.0f}" width="{w + 60:.0f}" height="27" /></bpmndi:BPMNLabel>'
    di.append(f'      <bpmndi:BPMNShape id="{i}_di" bpmnElement="{i}"{extra}><dc:Bounds x="{x:.0f}" y="{y:.0f}" width="{w}" height="{h}" />{lbl}</bpmndi:BPMNShape>')
for f in flows:
    s, t = nodes[f["s"]], nodes[f["t"]]
    sx, sy, sw, sh = bounds(s); tx, ty, tw, th = bounds(t)
    if s["cy"] == t["cy"]:
        pts = [(sx + sw, s["cy"]), (tx, t["cy"])]
    elif f["vfirst"] or s["kind"] == "gateway":
        up = t["cy"] < s["cy"]
        pts = [(s["cx"], sy if up else sy + sh), (s["cx"], t["cy"]), (tx, t["cy"])]
    else:
        up = t["cy"] < s["cy"]
        pts = [(sx + sw, s["cy"]), (t["cx"], s["cy"]), (t["cx"], ty + th if up else ty)]
    wp = "".join(f'<di:waypoint x="{px:.0f}" y="{py:.0f}" />' for px, py in pts)
    lbl = ""
    if f["name"]:
        lx, ly = pts[1] if len(pts) > 2 else pts[0]
        lbl = f'<bpmndi:BPMNLabel><dc:Bounds x="{lx + 6:.0f}" y="{ly - 20:.0f}" width="60" height="14" /></bpmndi:BPMNLabel>'
    di.append(f'      <bpmndi:BPMNEdge id="{f["id"]}_di" bpmnElement="{f["id"]}">{wp}{lbl}</bpmndi:BPMNEdge>')

xml = f'''<?xml version="1.0" encoding="UTF-8"?>
<!--
  Claybar Financials - Loan origination.
  Orchestrates KYC and credit-bureau checks in parallel over Kafka, applies the loan-risk DMN,
  routes borderline cases to manual underwriting and publishes the decision.
  Every Kafka interaction uses the claybar-workflow plug-in (kafkaCommandDelegate + message correlation).
-->
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI"
                  xmlns:dc="http://www.omg.org/spec/DD/20100524/DC"
                  xmlns:di="http://www.omg.org/spec/DD/20100524/DI"
                  xmlns:camunda="http://camunda.org/schema/1.0/bpmn"
                  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                  xmlns:modeler="http://camunda.org/schema/modeler/1.0"
                  id="Definitions_LoanOrigination"
                  targetNamespace="https://claybar.com/bpmn/loan-origination"
                  exporter="claybar-financials" exporterVersion="1.0"
                  modeler:executionPlatform="Camunda Platform" modeler:executionPlatformVersion="7.22.0">
  <bpmn:message id="Msg_LoanApplicationSubmitted" name="LoanApplicationSubmitted" />
  <bpmn:message id="Msg_KycCompleted" name="KycCompleted" />
  <bpmn:message id="Msg_CreditCheckCompleted" name="CreditCheckCompleted" />
  <bpmn:process id="loan-origination" name="Loan origination" isExecutable="true" camunda:historyTimeToLive="P180D">
{chr(10).join(proc)}
  </bpmn:process>
  <bpmndi:BPMNDiagram id="Diagram_LoanOrigination">
    <bpmndi:BPMNPlane id="Plane_LoanOrigination" bpmnElement="loan-origination">
{chr(10).join(di)}
    </bpmndi:BPMNPlane>
  </bpmndi:BPMNDiagram>
</bpmn:definitions>
'''
open(out, "w").write(xml)
print("wrote", out, len(nodes), "nodes", len(flows), "flows")
