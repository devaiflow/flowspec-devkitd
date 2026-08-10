//! Shared flow-selection logic for `FlowSource` implementations.

use flowspec_domain::flow::types::FlowDefinition;

/// Select the highest-version flow matching `name` and (optionally) a semver
/// `version_req`. Definitions with an unparseable version are still ordered
/// (falling back to lexicographic comparison) rather than dropped, so a
/// malformed version doesn't silently vanish from `list()`-derived views.
pub fn select_flow(
    defs: &[FlowDefinition],
    name: &str,
    version_req: Option<&str>,
) -> Option<FlowDefinition> {
    let mut matches: Vec<_> = defs.iter().filter(|f| f.name == name).cloned().collect();
    matches.retain(|f| {
        if let Some(req) = version_req {
            if let Ok(v) = semver::Version::parse(&f.version) {
                semver::VersionReq::parse(req)
                    .map(|r| r.matches(&v))
                    .unwrap_or(false)
            } else {
                false
            }
        } else {
            true
        }
    });
    matches.sort_by(|a, b| {
        let av = semver::Version::parse(&a.version).ok();
        let bv = semver::Version::parse(&b.version).ok();
        match (av, bv) {
            (Some(a), Some(b)) => b.cmp(&a), // highest first
            _ => a.version.cmp(&b.version).reverse(),
        }
    });
    matches.into_iter().next()
}
