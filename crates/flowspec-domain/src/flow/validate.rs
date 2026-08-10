use super::dag;
use super::schema::validate_fields;
use super::types::{FlowDefinition, StepWith};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    pub rule: &'static str,
    pub step: Option<String>,
    pub message: String,
}

fn v(rule: &'static str, step: Option<&str>, message: impl Into<String>) -> Violation {
    Violation {
        rule,
        step: step.map(String::from),
        message: message.into(),
    }
}

/// The full v0.3 flow-load rule checklist. Empty result means the flow is
/// structurally sound and safe to run.
pub fn validate(flow: &FlowDefinition) -> Vec<Violation> {
    let mut out = Vec::new();

    for fv in validate_fields(flow) {
        out.push(v(fv.rule, fv.step.as_deref(), fv.message));
    }

    unique_step_ids(flow, &mut out);
    type_with_consistency(flow, &mut out);
    routing_targets_exist(flow, &mut out);
    exactly_one_entry_step(flow, &mut out);
    acyclic(flow, &mut out);
    multi_target_needs(flow, &mut out);
    needs_matches_sources(flow, &mut out);
    human_loop_routing_pairs(flow, &mut out);
    reject_input_only_on_self_loop(flow, &mut out);
    no_cross_sibling_references(flow, &mut out);
    output_references_valid(flow, &mut out);
    no_direct_subflow_recursion(flow, &mut out);

    out
}

fn unique_step_ids(flow: &FlowDefinition, out: &mut Vec<Violation>) {
    let mut seen = HashSet::new();
    for step in &flow.steps {
        if !seen.insert(step.id.as_str()) {
            out.push(v(
                "unique_step_ids",
                Some(&step.id),
                format!("duplicate step id '{}'", step.id),
            ));
        }
    }
}

fn type_with_consistency(flow: &FlowDefinition, out: &mut Vec<Violation>) {
    for step in &flow.steps {
        if step.kind().is_none() {
            out.push(v(
                "type_with_consistency",
                Some(&step.id),
                format!(
                    "step '{}' has type '{}' but its 'with' block doesn't match that type's required shape",
                    step.id,
                    step.type_name()
                ),
            ));
        }
    }
}

fn routing_targets_exist(flow: &FlowDefinition, out: &mut Vec<Violation>) {
    for step in &flow.steps {
        for (field, route) in [
            ("on_success", &step.on_success),
            ("on_failure", &step.on_failure),
            ("on_approve", &step.on_approve),
            ("on_reject", &step.on_reject),
        ] {
            if let Some(route) = route {
                for target in route.as_slice() {
                    if !dag::target_is_valid(flow, target) {
                        out.push(v(
                            "routing_target_exists",
                            Some(&step.id),
                            format!("step '{}' {field} targets unknown step '{target}'", step.id),
                        ));
                    }
                }
            }
        }
        for needed in &step.needs {
            if flow.step(needed).is_none() {
                out.push(v(
                    "routing_target_exists",
                    Some(&step.id),
                    format!("step '{}' needs unknown step '{needed}'", step.id),
                ));
            }
        }
    }
}

fn exactly_one_entry_step(flow: &FlowDefinition, out: &mut Vec<Violation>) {
    if flow.steps.is_empty() {
        return;
    }
    let entries = dag::entry_steps(flow);
    if entries.len() != 1 {
        out.push(v(
            "exactly_one_entry_step",
            None,
            format!(
                "flow must have exactly one entry step (no incoming routing edges); found {}: [{}]",
                entries.len(),
                entries.join(", ")
            ),
        ));
    }
}

fn acyclic(flow: &FlowDefinition, out: &mut Vec<Violation>) {
    if let Some(cycle) = dag::find_cycle(flow) {
        out.push(v(
            "acyclic",
            None,
            format!("routing graph contains a cycle: {}", cycle.join(" -> ")),
        ));
    }
}

fn multi_target_needs(flow: &FlowDefinition, out: &mut Vec<Violation>) {
    let incoming = dag::steps_with_incoming_edges(flow);
    for (&target, sources) in &incoming {
        if sources.len() > 1 {
            let Some(step) = flow.step(target) else {
                continue;
            };
            if step.needs.is_empty() {
                out.push(v(
                    "multi_target_needs_join",
                    Some(target),
                    format!(
                        "step '{target}' is targeted by {} routing sources but declares no needs:",
                        sources.len()
                    ),
                ));
            }
        }
    }
}

fn needs_matches_sources(flow: &FlowDefinition, out: &mut Vec<Violation>) {
    let incoming = dag::steps_with_incoming_edges(flow);
    for step in &flow.steps {
        if step.needs.is_empty() {
            continue;
        }
        let sources: HashSet<&str> = incoming
            .get(step.id.as_str())
            .map(|v| v.iter().copied().collect())
            .unwrap_or_default();
        for needed in &step.needs {
            if !sources.contains(needed.as_str()) {
                out.push(v(
                    "needs_matches_sources",
                    Some(&step.id),
                    format!(
                        "step '{}' declares needs '{needed}' but '{needed}' does not route to it",
                        step.id
                    ),
                ));
            }
        }
    }
}

fn human_loop_routing_pairs(flow: &FlowDefinition, out: &mut Vec<Violation>) {
    for step in &flow.steps {
        if step.human_loop {
            if step.on_approve.is_none() || step.on_reject.is_none() {
                out.push(v(
                    "human_loop_routing_pairs",
                    Some(&step.id),
                    format!(
                        "step '{}' has human_loop: true and must declare both on_approve and on_reject",
                        step.id
                    ),
                ));
            }
        } else if step.on_approve.is_some() || step.on_reject.is_some() {
            out.push(v(
                "human_loop_routing_pairs",
                Some(&step.id),
                format!(
                    "step '{}' declares on_approve/on_reject without human_loop: true",
                    step.id
                ),
            ));
        }
    }
}

fn reject_input_only_on_self_loop(flow: &FlowDefinition, out: &mut Vec<Violation>) {
    for step in &flow.steps {
        if step.reject_input.is_some() && !dag::is_self_reject_loop(step) {
            out.push(v(
                "reject_input_self_loop_only",
                Some(&step.id),
                format!(
                    "step '{}' declares reject_input but on_reject does not self-loop",
                    step.id
                ),
            ));
        }
    }
}

fn no_cross_sibling_references(flow: &FlowDefinition, out: &mut Vec<Violation>) {
    // A step referencing `{{ steps.Y.output }}` where Y is a parallel sibling
    // (Y is reachable only through a fan-out branch that does not gate this step
    // via `needs:`) is a race. We approximate: for each step, the set of steps
    // guaranteed to have completed before it is those in transitive `needs:`
    // closure plus the unique predecessor chain. Any `{{ steps.<id>. }}` reference
    // in `input`/`reject_input` templates to a step outside that set is flagged.
    let guaranteed: HashMap<&str, HashSet<&str>> = flow
        .steps
        .iter()
        .map(|s| (s.id.as_str(), guaranteed_predecessors(flow, &s.id)))
        .collect();

    for step in &flow.steps {
        let refs = step_template_step_refs(step);
        let allowed = guaranteed
            .get(step.id.as_str())
            .cloned()
            .unwrap_or_default();
        for referenced in refs {
            if referenced == step.id {
                continue;
            }
            if !allowed.contains(referenced.as_str()) {
                out.push(v(
                    "no_cross_sibling_references",
                    Some(&step.id),
                    format!(
                        "step '{}' references steps.{referenced}, which is not a guaranteed predecessor (needs: or sole routing chain)",
                        step.id
                    ),
                ));
            }
        }
    }
}

/// Steps guaranteed to have completed before `step_id` activates: everything
/// reachable backwards through `needs:` (transitively), plus — for steps
/// reached by a single non-fan-out predecessor chain — that chain.
fn guaranteed_predecessors<'a>(flow: &'a FlowDefinition, step_id: &str) -> HashSet<&'a str> {
    let incoming = dag::steps_with_incoming_edges(flow);
    let mut result: HashSet<&str> = HashSet::new();
    let mut stack: Vec<&str> = Vec::new();

    let sources = incoming.get(step_id).cloned().unwrap_or_default();
    if sources.len() == 1 {
        stack.push(sources[0]);
    } else if let Some(step) = flow.step(step_id) {
        // Fan-in: only the explicit `needs:` set is guaranteed.
        for needed in &step.needs {
            if let Some(s) = flow.step(needed) {
                stack.push(s.id.as_str());
            }
        }
    }

    while let Some(node) = stack.pop() {
        if result.insert(node) {
            let node_sources = incoming.get(node).cloned().unwrap_or_default();
            if node_sources.len() == 1 {
                stack.push(node_sources[0]);
            } else if let Some(step) = flow.step(node) {
                for needed in &step.needs {
                    if let Some(s) = flow.step(needed) {
                        stack.push(s.id.as_str());
                    }
                }
            }
        }
    }

    result
}

/// Extracts `steps.<id>` references from a step's templated string fields.
fn step_template_step_refs(step: &super::types::Step) -> Vec<String> {
    let mut inputs = Vec::new();
    if let Some(StepWith::Cli { with }) = step.kind() {
        inputs.push(with.input.clone());
    }
    if let Some(ri) = &step.reject_input {
        inputs.push(ri.clone());
    }
    inputs.iter().flat_map(|s| extract_step_refs(s)).collect()
}

fn extract_step_refs(text: &str) -> Vec<String> {
    let mut refs = Vec::new();
    let mut rest = text;
    while let Some(start) = rest.find("{{") {
        let after = &rest[start + 2..];
        let Some(end) = after.find("}}") else { break };
        let expr = after[..end].trim();
        if let Some(path) = expr.strip_prefix("steps.")
            && let Some(id) = path.split('.').next()
        {
            refs.push(id.to_string());
        }
        rest = &after[end + 2..];
    }
    refs
}

/// Catches a flow directly invoking itself as a subflow. Transitive recursion
/// (A -> B -> A across different flow files) needs the full flow registry and is
/// checked by the flow-source loader, not here.
fn no_direct_subflow_recursion(flow: &FlowDefinition, out: &mut Vec<Violation>) {
    for step in &flow.steps {
        if let Some(StepWith::Subflow { with }) = step.kind()
            && with.flow == flow.name
        {
            out.push(v(
                    "no_subflow_recursion",
                    Some(&step.id),
                    format!(
                        "step '{}' invokes '{}' as a subflow, which is this same flow (direct recursion is forbidden)",
                        step.id, with.flow
                    ),
                ));
        }
    }
}

fn output_references_valid(flow: &FlowDefinition, out: &mut Vec<Violation>) {
    // Static check: for `{{ steps.<id>.output.<field> }}` where <id> is a subflow
    // step, verify <field> is among the target flow's declared outputs -- but the
    // target flow's own definition isn't available here (only this flow is), so
    // we only check dotted access isn't used against steps that are NOT subflow
    // steps declaring named outputs (cli steps only expose `.output` as a whole).
    for step in &flow.steps {
        let refs = step_template_dotted_output_refs(step);
        for (referenced, has_field) in refs {
            if !has_field {
                continue;
            }
            match flow.step(&referenced).and_then(|s| s.kind()) {
                Some(StepWith::Cli { .. }) => {
                    out.push(v(
                        "output_reference_valid",
                        Some(&step.id),
                        format!(
                            "step '{}' references steps.{referenced}.output.<field>, but '{referenced}' is a cli step (output is a single value, not a named-field object)",
                            step.id
                        ),
                    ));
                }
                None => {
                    // Already reported by routing_targets_exist / cross-sibling checks.
                }
                _ => {}
            }
        }
    }
}

fn step_template_dotted_output_refs(step: &super::types::Step) -> Vec<(String, bool)> {
    let mut inputs = Vec::new();
    if let Some(StepWith::Cli { with }) = step.kind() {
        inputs.push(with.input.clone());
    }
    if let Some(ri) = &step.reject_input {
        inputs.push(ri.clone());
    }
    let mut result = Vec::new();
    for text in &inputs {
        let mut rest = text.as_str();
        while let Some(start) = rest.find("{{") {
            let after = &rest[start + 2..];
            let Some(end) = after.find("}}") else { break };
            let expr = after[..end].trim();
            if let Some(path) = expr.strip_prefix("steps.") {
                let mut parts = path.split('.');
                if let Some(id) = parts.next() {
                    let remainder: Vec<&str> = parts.collect();
                    let has_field = remainder.len() > 1 && remainder[0] == "output";
                    result.push((id.to_string(), has_field));
                }
            }
            rest = &after[end + 2..];
        }
    }
    result
}
