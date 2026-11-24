use std::{
    num::{NonZeroU32, NonZeroUsize},
    time::Duration,
};

use serde_with::serde_as;
use vector_lib::{config::clone_input_definitions, configurable::configurable_component};

use super::transform::FairThrottle;
use crate::{
    config::{DataType, Input, OutputId, TransformConfig, TransformContext, TransformOutput},
    schema,
    template::UnconfinedTemplate,
    transforms::Transform,
};

/// Configuration of internal metrics for the Fair Throttle transform.
#[configurable_component]
#[derive(Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct FairThrottleInternalMetricsConfig {
    /// Include a `group` tag on queue admission, emission, and discard counters.
    ///
    /// Missing or unrenderable groups use `None`. Only enable this for a bounded set of group
    /// values: limiting concurrent queues does not limit metric cardinality over time.
    #[serde(default)]
    pub include_group_tag: bool,
}

/// Configuration for the `fair_throttle` transform.
#[serde_as]
#[configurable_component(transform(
    "fair_throttle",
    "Share a log rate limit fairly between weighted groups."
))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct FairThrottleConfig {
    /// The sustained number of log events allowed per second across all groups.
    ///
    /// Each event costs one unit, regardless of its size. The limit applies at this transform's
    /// output; downstream buffering and fanout can change the timing and number of exported copies.
    #[configurable(validation(range(max = 1_000_000_000)))]
    pub rate_per_sec: NonZeroU32,

    /// Maximum burst of events allowed by the shared rate limiter.
    ///
    /// Defaults to one millisecond of output capacity, rounded up, to accommodate timer
    /// granularity. Set this to `1` to disable bursts, at the cost of lower throughput at high
    /// rates. This must not exceed `rate_per_sec`. The rate limit permits at most
    /// `burst + rate_per_sec * elapsed_seconds` events, rather than a strict rolling-second cap.
    pub burst: Option<NonZeroU32>,

    /// The value used to group events into independently queued services.
    ///
    /// Use a stable service identity, such as namespace and workload, rather than a pod or request
    /// identifier. Missing or unrenderable values share a fallback group. If omitted, all events
    /// share one group.
    #[configurable(metadata(docs::examples = "{{ service }}"))]
    pub group_by: Option<UnconfinedTemplate>,

    /// Default scheduling weight, also used when `weight_field` is missing or invalid.
    ///
    /// Larger weights receive proportionally more output capacity when groups remain backlogged.
    /// Unused capacity is available to other groups. Weights apply to retained event counts.
    #[serde(default = "default_weight")]
    pub weight: NonZeroU32,

    /// The event field whose positive integer value supplies its group's scheduling weight.
    ///
    /// Accepts integers or strings containing a positive 32-bit integer. Floats, zero, negative,
    /// missing, and invalid values fall back to `weight`, following `sample.rate_field` semantics.
    /// The latest observed weight for a group applies at the start of its next scheduling turn,
    /// including when the observed event is dropped because the queue is full. Updating a weight
    /// does not replenish the current turn's credit. Prefer a consistent weight on all events
    /// belonging to a service. Larger weights can increase other groups' waiting time.
    #[configurable(metadata(docs::examples = "log_weight"))]
    pub weight_field: Option<String>,

    /// Maximum number of simultaneously queued groups.
    ///
    /// Events for additional groups are dropped until an existing queue empties or expires.
    /// Each admitted group has its own event and byte allowance, so a busy group cannot consume
    /// another group's queue capacity. Choose a limit that accommodates all expected services.
    #[serde(default = "default_max_groups")]
    pub max_groups: NonZeroUsize,

    /// Maximum number of queued events per group.
    ///
    /// When a group's queue is full, newly arriving events for that group are intentionally
    /// dropped. Smaller queues reduce memory and latency but tolerate smaller input bursts.
    #[serde(default = "default_max_events_per_group")]
    pub max_events_per_group: NonZeroUsize,

    /// Maximum estimated in-memory event bytes plus rendered key bytes per group.
    ///
    /// Events that would exceed this limit are dropped, including individual oversized events.
    /// Total retained event and key bytes are bounded by `max_groups * max_bytes_per_group`.
    /// Queue and allocator bookkeeping consume additional memory bounded by the group and event
    /// limits. This is not a serialized-byte rate limit.
    #[serde(default = "default_max_bytes_per_group")]
    #[configurable(metadata(docs::type_unit = "bytes"))]
    pub max_bytes_per_group: NonZeroUsize,

    /// Maximum time an event may wait in a service queue, in seconds.
    ///
    /// Expired events are intentionally dropped. Queues drain at the configured rate when input
    /// closes, with this age limit still enforced. Queues are held in memory and are not persisted
    /// across restarts. Downstream backpressure can pause input and expiration processing; expired
    /// events are discarded before output resumes.
    #[serde(default = "default_max_age_secs")]
    #[serde_as(as = "serde_with::DurationSecondsWithFrac<f64>")]
    pub max_age_secs: Duration,

    /// Configuration of internal metrics for the Fair Throttle transform.
    #[serde(default)]
    pub internal_metrics: FairThrottleInternalMetricsConfig,
}

const fn default_weight() -> NonZeroU32 {
    NonZeroU32::MIN
}
const fn default_max_groups() -> NonZeroUsize {
    NonZeroUsize::new(128).expect("non-zero constant")
}
const fn default_max_events_per_group() -> NonZeroUsize {
    NonZeroUsize::new(16).expect("non-zero constant")
}
const fn default_max_bytes_per_group() -> NonZeroUsize {
    NonZeroUsize::new(256 * 1024).expect("non-zero constant")
}
const fn default_max_age_secs() -> Duration {
    Duration::from_secs(1)
}

impl Default for FairThrottleConfig {
    fn default() -> Self {
        Self {
            rate_per_sec: NonZeroU32::new(1000).expect("non-zero constant"),
            burst: None,
            group_by: None,
            weight: default_weight(),
            weight_field: None,
            max_groups: default_max_groups(),
            max_events_per_group: default_max_events_per_group(),
            max_bytes_per_group: default_max_bytes_per_group(),
            max_age_secs: default_max_age_secs(),
            internal_metrics: FairThrottleInternalMetricsConfig::default(),
        }
    }
}

impl FairThrottleConfig {
    pub(super) fn burst_size(&self) -> u32 {
        self.burst
            .map_or_else(|| self.rate_per_sec.get().div_ceil(1000), NonZeroU32::get)
    }
}

impl_generate_config_from_default!(FairThrottleConfig);

#[async_trait::async_trait]
#[typetag::serde(name = "fair_throttle")]
impl TransformConfig for FairThrottleConfig {
    async fn build(&self, _: &TransformContext) -> crate::Result<Transform> {
        self.validate_structure()
            .map_err(|errors| errors.join("; "))?;
        Ok(Transform::event_task(FairThrottle::new(self.clone())))
    }

    fn input(&self) -> Input {
        Input::log()
    }

    fn outputs(
        &self,
        _: &TransformContext,
        definitions: &[(OutputId, schema::Definition)],
    ) -> Vec<TransformOutput> {
        vec![TransformOutput::new(
            DataType::Log,
            clone_input_definitions(definitions),
        )]
    }

    fn validate_structure(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();
        if self.rate_per_sec.get() > 1_000_000_000 {
            errors.push("`rate_per_sec` must not exceed 1,000,000,000".to_owned());
        }
        if self.burst_size() > self.rate_per_sec.get() {
            errors.push("`burst` must not exceed `rate_per_sec`".to_owned());
        }
        if self.max_age_secs.is_zero()
            || std::time::Instant::now()
                .checked_add(self.max_age_secs)
                .is_none()
        {
            errors.push(
                "`max_age_secs` must be positive and representable by the monotonic clock"
                    .to_owned(),
            );
        }
        if self
            .max_groups
            .get()
            .checked_mul(self.max_bytes_per_group.get())
            .is_none()
            || self
                .max_groups
                .get()
                .checked_mul(self.max_events_per_group.get())
                .is_none()
        {
            errors.push("combined queue limits must fit in usize".to_owned());
        }
        if self
            .weight_field
            .as_ref()
            .is_some_and(|field| vrl::path::parse_target_path(field).is_err())
        {
            errors.push("`weight_field` must be a valid event field path".to_owned());
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}
