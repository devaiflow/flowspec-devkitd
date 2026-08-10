use super::types::{DONE, FlowDefinition, RouteTarget, Step};
use std::collections::{HashMap, HashSet};

/// All `on_*` targets of a step, excluding the literal `"done"`.
pub fn routing_targets(step: &Step) -> Vec<&str> {
    let mut targets = Vec::new();
    for route in [
        &step.on_success,
        &step.on_failure,
        &step.on_approve,
        &step.on_reject,
    ]
    .into_iter()
    .flatten()
    {
        targets.extend(route.as_slice().into_iter().filter(|t| *t != DONE));
    }
    targets
}

/// Whether this step's `on_reject` targets itself (the one allowed self-loop).
pub fn is_self_reject_loop(step: &Step) -> bool {
    step.on_reject
        .as_ref()
        .map(|r| r.as_slice() == vec![step.id.as_str()])
        .unwrap_or(false)
}

/// Adjacency: step id -> ids it routes to (deduplicated), excluding the self-reject-loop edge.
pub fn adjacency(flow: &FlowDefinition) -> HashMap<&str, HashSet<&str>> {
    let mut adj: HashMap<&str, HashSet<&str>> = HashMap::new();
    for step in &flow.steps {
        let entry = adj.entry(step.id.as_str()).or_default();
        for target in routing_targets(step) {
            if is_self_reject_loop(step) && target == step.id {
                continue;
            }
            entry.insert(target);
        }
    }
    adj
}

/// Step ids that are the target of at least one routing edge (from any source, any field).
pub fn steps_with_incoming_edges(flow: &FlowDefinition) -> HashMap<&str, Vec<&str>> {
    let mut incoming: HashMap<&str, Vec<&str>> = HashMap::new();
    for step in &flow.steps {
        for route in [
            &step.on_success,
            &step.on_failure,
            &step.on_approve,
            &step.on_reject,
        ]
        .into_iter()
        .flatten()
        {
            for target in route.as_slice() {
                if target == DONE {
                    continue;
                }
                if is_self_reject_loop(step) && target == step.id {
                    continue;
                }
                incoming.entry(target).or_default().push(step.id.as_str());
            }
        }
    }
    incoming
}

/// The entry step: the one step no other step routes to. Tries the strict
/// reading first -- zero incoming edges of *any* kind -- and only falls back to
/// forward-progress-only incoming edges (excluding `on_failure`) if that strict
/// set is empty. The fallback matters for retry loops like `implement
/// -on_success-> test -on_failure-> implement`: under the strict, all-edges
/// reading both steps have an incoming edge and no entry remains, even though
/// `implement` is clearly the intended start. A step reachable *only* via
/// `on_failure` (a pure failure sink, e.g. `report-failure` fed by several
/// branches' `on_failure`) must still disqualify as entry under the strict
/// reading, which the fallback alone would miss.
pub fn entry_steps(flow: &FlowDefinition) -> Vec<&str> {
    let incoming = steps_with_incoming_edges(flow);
    let strict: Vec<&str> = flow
        .steps
        .iter()
        .map(|s| s.id.as_str())
        .filter(|id| !incoming.contains_key(id))
        .collect();
    if !strict.is_empty() {
        return strict;
    }

    let forward_incoming: HashSet<&str> = forward_adjacency(flow)
        .values()
        .flat_map(|targets| targets.iter().copied())
        .collect();
    flow.steps
        .iter()
        .map(|s| s.id.as_str())
        .filter(|id| !forward_incoming.contains(id))
        .collect()
}

/// Adjacency over `on_success`/`on_approve`/`on_reject` edges only (self-reject-loop
/// excluded). `on_failure` is deliberately omitted: it is a recovery edge, not a
/// forward-progress edge, and the spec's own canonical example loops a failing
/// `test` step back to `implement` via `on_failure` — that is retry semantics,
/// not a structural cycle.
fn forward_adjacency(flow: &FlowDefinition) -> HashMap<&str, HashSet<&str>> {
    let mut adj: HashMap<&str, HashSet<&str>> = HashMap::new();
    for step in &flow.steps {
        let entry = adj.entry(step.id.as_str()).or_default();
        for route in [&step.on_success, &step.on_approve, &step.on_reject]
            .into_iter()
            .flatten()
        {
            for target in route.as_slice() {
                if target == DONE {
                    continue;
                }
                if is_self_reject_loop(step) && target == step.id {
                    continue;
                }
                entry.insert(target);
            }
        }
    }
    adj
}

/// Detects a cycle in the forward-progress routing graph (self-reject-loops and
/// `on_failure` edges excluded). Returns the cycle path (step ids) if one exists.
pub fn find_cycle(flow: &FlowDefinition) -> Option<Vec<String>> {
    let adj = forward_adjacency(flow);

    #[derive(Clone, Copy, PartialEq)]
    enum Color {
        White,
        Gray,
        Black,
    }

    let mut color: HashMap<&str, Color> = flow
        .steps
        .iter()
        .map(|s| (s.id.as_str(), Color::White))
        .collect();
    let mut stack: Vec<&str> = Vec::new();

    fn visit<'a>(
        node: &'a str,
        adj: &HashMap<&'a str, HashSet<&'a str>>,
        color: &mut HashMap<&'a str, Color>,
        stack: &mut Vec<&'a str>,
    ) -> Option<Vec<String>> {
        color.insert(node, Color::Gray);
        stack.push(node);

        if let Some(neighbors) = adj.get(node) {
            let mut sorted: Vec<&&str> = neighbors.iter().collect();
            sorted.sort();
            for &next in sorted {
                match color.get(next).copied().unwrap_or(Color::White) {
                    Color::White => {
                        if let Some(cycle) = visit(next, adj, color, stack) {
                            return Some(cycle);
                        }
                    }
                    Color::Gray => {
                        let start = stack.iter().position(|&n| n == next).unwrap_or(0);
                        let mut cycle: Vec<String> =
                            stack[start..].iter().map(|s| s.to_string()).collect();
                        cycle.push(next.to_string());
                        return Some(cycle);
                    }
                    Color::Black => {}
                }
            }
        }

        stack.pop();
        color.insert(node, Color::Black);
        None
    }

    let mut ids: Vec<&str> = flow.steps.iter().map(|s| s.id.as_str()).collect();
    ids.sort();
    for id in ids {
        if color.get(id).copied().unwrap_or(Color::White) == Color::White
            && let Some(cycle) = visit(id, &adj, &mut color, &mut stack)
        {
            return Some(cycle);
        }
    }
    None
}

/// Steps reachable from `start`, following routing edges (self-loop excluded).
pub fn reachable_from<'a>(flow: &'a FlowDefinition, start: &str) -> HashSet<&'a str> {
    let adj = adjacency(flow);
    let mut seen: HashSet<&str> = HashSet::new();
    let Some(start_step) = flow.step(start) else {
        return seen;
    };
    let mut stack = vec![start_step.id.as_str()];
    while let Some(node) = stack.pop() {
        if seen.insert(node)
            && let Some(neighbors) = adj.get(node)
        {
            for &n in neighbors {
                stack.push(n);
            }
        }
    }
    seen
}

pub fn target_is_valid(flow: &FlowDefinition, target: &str) -> bool {
    target == DONE || flow.step(target).is_some()
}

/// Whether `RouteTarget` refers to `step_id` as a `RouteTarget::Many` implying fan-out
/// (more than one target).
pub fn is_fan_out(route: &Option<RouteTarget>) -> bool {
    matches!(route, Some(RouteTarget::Many(v)) if v.len() > 1)
}
