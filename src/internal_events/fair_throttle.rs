use metrics::Label;
use vector_lib::{
    NamedInternalEvent, counter, gauge,
    internal_event::{
        ComponentEventsDropped, CounterName, GaugeName, INTENTIONAL, InternalEvent, UNINTENTIONAL,
    },
};

fn labels(group: Option<String>) -> Vec<Label> {
    let mut labels = vec![
        Label::new("buffer_type", "fair_throttle"),
        Label::new("buffer_id", "fair_throttle"),
        Label::new("stage", "0"),
    ];
    if let Some(group) = group {
        labels.push(Label::new("group", group));
    }
    labels
}

#[derive(Debug, NamedInternalEvent)]
pub(crate) struct FairThrottleEventQueued {
    pub group: Option<String>,
}

impl InternalEvent for FairThrottleEventQueued {
    fn emit(self) {
        counter!(CounterName::BufferReceivedEventsTotal, labels(self.group)).increment(1);
    }
}

#[derive(Debug, NamedInternalEvent)]
pub(crate) struct FairThrottleEventSent {
    pub group: Option<String>,
}

impl InternalEvent for FairThrottleEventSent {
    fn emit(self) {
        counter!(CounterName::BufferSentEventsTotal, labels(self.group)).increment(1);
    }
}

#[derive(Debug, NamedInternalEvent)]
pub(crate) struct FairThrottleEventsDropped {
    pub group: Option<String>,
    pub count: usize,
    pub reason: &'static str,
    pub intentional: bool,
}

impl InternalEvent for FairThrottleEventsDropped {
    fn emit(self) {
        let mut tags = labels(self.group.clone());
        tags.push(Label::new("reason", self.reason));
        tags.push(Label::new(
            "intentional",
            if self.intentional { "true" } else { "false" },
        ));
        counter!(CounterName::BufferDiscardedEventsTotal, tags).increment(self.count as u64);
        if self.intentional {
            ComponentEventsDropped::<INTENTIONAL> {
                count: self.count,
                reason: self.reason,
            }
            .emit_with_group(self.group);
        } else {
            ComponentEventsDropped::<UNINTENTIONAL> {
                count: self.count,
                reason: self.reason,
            }
            .emit_with_group(self.group);
        }
    }
}

#[derive(Debug, NamedInternalEvent)]
pub(crate) struct FairThrottleQueueState {
    pub events: usize,
    pub bytes: usize,
}

impl InternalEvent for FairThrottleQueueState {
    #[allow(
        clippy::cast_precision_loss,
        reason = "Queue metrics are approximate floating-point gauges."
    )]
    fn emit(self) {
        gauge!(GaugeName::BufferSizeEvents, labels(None)).set(self.events as f64);
        gauge!(GaugeName::BufferSizeBytes, labels(None)).set(self.bytes as f64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vector_lib::{event::MetricValue, metrics::Controller};

    #[test]
    #[serial_test::serial]
    fn emits_per_service_queue_counters_with_buffer_labels() {
        vector_lib::metrics::init_test();
        let group = Some("fair-throttle-metric-test".to_owned());
        FairThrottleEventQueued {
            group: group.clone(),
        }
        .emit();
        FairThrottleEventSent {
            group: group.clone(),
        }
        .emit();
        FairThrottleEventsDropped {
            group,
            count: 2,
            reason: "Service event queue is full.",
            intentional: true,
        }
        .emit();
        let metrics = Controller::get().unwrap().capture_metrics();
        for (name, expected) in [
            ("buffer_received_events_total", 1.0),
            ("buffer_sent_events_total", 1.0),
            ("buffer_discarded_events_total", 2.0),
        ] {
            let metric = metrics
                .iter()
                .find(|metric| {
                    metric.name() == name
                        && metric.tags().is_some_and(|tags| {
                            tags.get("group") == Some("fair-throttle-metric-test")
                        })
                })
                .unwrap();
            let tags = metric.tags().unwrap();
            assert_eq!(tags.get("buffer_id"), Some("fair_throttle"));
            assert_eq!(tags.get("stage"), Some("0"));
            assert_eq!(metric.value(), &MetricValue::Counter { value: expected });
        }
    }
}
