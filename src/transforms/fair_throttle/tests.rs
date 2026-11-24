use std::{
    num::{NonZeroU32, NonZeroUsize},
    task::Poll,
    time::Duration,
};

use futures::{StreamExt, stream};
use tokio::time::Instant;
use vector_lib::{
    ByteSizeOf,
    event::{BatchNotifier, BatchStatus},
};
use vrl::event_path;

use super::{
    config::FairThrottleConfig,
    scheduler::{Group, RateGate, Scheduler},
    transform::FairThrottle,
};
use crate::{
    config::{TransformConfig, TransformContext},
    event::{Event, LogEvent, Value},
    transforms::TaskTransform,
};

fn weight(value: u32) -> NonZeroU32 {
    NonZeroU32::new(value).unwrap()
}
#[allow(
    clippy::unnecessary_wraps,
    reason = "Use the scheduler key type, which also represents missing groups."
)]
fn group(value: &str) -> Group {
    Some(value.into())
}

fn event(service: &str, id: i64, weight: i64) -> Event {
    let mut log = LogEvent::from("message");
    log.insert(event_path!("service"), service);
    log.insert(event_path!("id"), id);
    log.insert(event_path!("log_weight"), weight);
    log.into()
}

fn service(event: &Event) -> String {
    event
        .as_log()
        .get(event_path!("service"))
        .unwrap()
        .to_string_lossy()
        .into_owned()
}

fn config() -> FairThrottleConfig {
    serde_yaml::from_str("rate_per_sec: 100\nburst: 1\ngroup_by: '{{ service }}'\nweight_field: log_weight\nmax_events_per_group: 1000\nmax_age_secs: 60\n").unwrap()
}

#[test]
fn generate_config() {
    crate::test_util::test_generate_config::<FairThrottleConfig>();
}

#[test]
fn validates_limits_and_field_paths() {
    for yaml in [
        "rate_per_sec: 0",
        "rate_per_sec: 1\nburst: 0",
        "rate_per_sec: 1\nweight: 0",
        "rate_per_sec: 1\nmax_groups: 0",
        "rate_per_sec: 1\nmax_events_per_group: 0",
        "rate_per_sec: 1\nmax_bytes_per_group: 0",
    ] {
        assert!(
            serde_yaml::from_str::<FairThrottleConfig>(yaml).is_err(),
            "{yaml}"
        );
    }
    for yaml in [
        "rate_per_sec: 1000000001",
        "rate_per_sec: 1\nburst: 2",
        "rate_per_sec: 1\nmax_age_secs: 0",
        "rate_per_sec: 1\nweight_field: '['",
    ] {
        assert!(
            serde_yaml::from_str::<FairThrottleConfig>(yaml)
                .unwrap()
                .validate_structure()
                .is_err(),
            "{yaml}"
        );
    }
    let mut config = config();
    config.max_groups = NonZeroUsize::MAX;
    assert!(config.validate_structure().is_err());
}

#[test]
fn event_weights_follow_sample_integer_field_semantics() {
    let mut config = config();
    config.weight = weight(7);
    let transform = FairThrottle::new(config);
    for (value, expected) in [
        (Value::from(3), 3),
        (Value::from("4"), 4),
        (Value::from(0), 7),
        (Value::from(-1), 7),
        (Value::from(3.0), 7),
        (Value::from("0"), 7),
        (Value::from("1.5"), 7),
        (Value::from("bad"), 7),
        (Value::from(true), 7),
        (Value::Null, 7),
        (Value::from(i64::from(u32::MAX) + 1), 7),
    ] {
        let mut event = event("a", 0, 1);
        event.as_mut_log().insert(event_path!("log_weight"), value);
        assert_eq!(transform.event_weight(&event).get(), expected);
    }
    assert_eq!(transform.event_weight(&LogEvent::default().into()).get(), 7);
}

#[test]
fn nested_weight_fields_and_missing_groups() {
    let mut config = config();
    config.weight_field = Some("policy.weight".into());
    let transform = FairThrottle::new(config);
    let mut event = event("a", 0, 1);
    event
        .as_mut_log()
        .insert(event_path!("policy", "weight"), 5);
    assert_eq!(transform.event_weight(&event).get(), 5);
    assert_eq!(transform.group(&event), group("a"));
    assert_eq!(transform.group(&LogEvent::default().into()), None);
}

#[test]
fn saturated_groups_get_weighted_shares_and_preserve_fifo() {
    let mut scheduler = Scheduler::new(config());
    let now = Instant::now();
    for id in 0..300 {
        scheduler.enqueue(&group("a"), weight(1), event("a", id, 1), now);
        scheduler.enqueue(&group("b"), weight(3), event("b", id, 3), now);
    }
    let mut counts = [0_i64; 2];
    for index in 0..400 {
        let event = scheduler.pop().unwrap();
        let slot = usize::from(index % 4 != 0);
        assert_eq!(service(&event), if slot == 0 { "a" } else { "b" });
        assert_eq!(
            event.as_log().get(event_path!("id")),
            Some(&Value::from(counts[slot]))
        );
        counts[slot] += 1;
    }
    assert_eq!(counts, [100, 300]);
    // Once B drains, A borrows all remaining capacity.
    for _ in 0..200 {
        assert_eq!(service(&scheduler.pop().unwrap()), "a");
    }
    assert!(scheduler.pop().is_none());
    assert_eq!(scheduler.size(), (0, 0, 0));
}

#[test]
fn weight_updates_apply_next_turn_without_refilling_current_credit() {
    let mut scheduler = Scheduler::new(config());
    let now = Instant::now();
    for id in 0..10 {
        scheduler.enqueue(&group("a"), weight(2), event("a", id, 2), now);
        scheduler.enqueue(&group("b"), weight(1), event("b", id, 1), now);
    }
    assert_eq!(service(&scheduler.pop().unwrap()), "a");
    scheduler.enqueue(&group("a"), weight(3), event("a", 10, 3), now);
    for expected in ["a", "b", "a", "a", "a", "b"] {
        assert_eq!(service(&scheduler.pop().unwrap()), expected);
    }
}

#[test]
fn full_queue_still_observes_new_service_weight() {
    let mut config = config();
    config.max_events_per_group = NonZeroUsize::new(4).unwrap();
    let mut scheduler = Scheduler::new(config);
    let now = Instant::now();
    for id in 0..4 {
        scheduler.enqueue(&group("a"), weight(1), event("a", id, 1), now);
        scheduler.enqueue(&group("b"), weight(1), event("b", id, 1), now);
    }
    scheduler.enqueue(&group("a"), weight(3), event("a", 99, 3), now);
    for expected in ["a", "a", "a", "b"] {
        assert_eq!(service(&scheduler.pop().unwrap()), expected);
    }
}

#[test]
fn empty_queues_do_not_bank_credit() {
    let mut scheduler = Scheduler::new(config());
    let now = Instant::now();
    scheduler.enqueue(&group("a"), weight(10), event("a", 0, 10), now);
    scheduler.enqueue(&group("b"), weight(1), event("b", 0, 1), now);
    assert_eq!(service(&scheduler.pop().unwrap()), "a");
    scheduler.enqueue(&group("a"), weight(1), event("a", 1, 1), now);
    assert_eq!(service(&scheduler.pop().unwrap()), "b");
    assert_eq!(service(&scheduler.pop().unwrap()), "a");
    assert!(scheduler.is_empty());
}

#[test]
fn event_capacity_is_protected_per_service() {
    let mut config = config();
    config.max_events_per_group = NonZeroUsize::new(2).unwrap();
    let mut scheduler = Scheduler::new(config);
    let now = Instant::now();
    for id in 0..100 {
        scheduler.enqueue(&group("a"), weight(1), event("a", id, 1), now);
    }
    scheduler.enqueue(&group("b"), weight(1), event("b", 0, 1), now);
    assert_eq!(scheduler.size().1, 3);
    for (expected, id) in [("a", 0), ("b", 0), ("a", 1)] {
        let event = scheduler.pop().unwrap();
        assert_eq!(service(&event), expected);
        assert_eq!(
            event.as_log().get(event_path!("id")),
            Some(&Value::from(id))
        );
    }
    assert_eq!(scheduler.size(), (0, 0, 0));
}

#[test]
fn byte_limit_includes_keys_and_does_not_allocate_oversized_groups() {
    let event = event("a", 0, 1);
    let bytes = event.size_of();
    let mut config = config();
    config.max_bytes_per_group = NonZeroUsize::new(bytes + 1).unwrap();
    let mut scheduler = Scheduler::new(config);
    let now = Instant::now();
    scheduler.enqueue(&group("long-key"), weight(1), event.clone(), now);
    assert_eq!(scheduler.size(), (0, 0, 0));
    scheduler.enqueue(&group("a"), weight(1), event.clone(), now);
    scheduler.enqueue(&group("a"), weight(1), event.clone(), now);
    scheduler.enqueue(&group("b"), weight(1), event, now);
    assert_eq!(scheduler.size(), (2, 2, 2 * (bytes + 1)));
    scheduler.pop().unwrap();
    scheduler.pop().unwrap();
    assert_eq!(scheduler.size(), (0, 0, 0));
}

#[test]
fn group_limit_is_reclaimed_when_queues_empty_or_expire() {
    let mut config = config();
    config.max_groups = NonZeroUsize::new(2).unwrap();
    config.max_age_secs = Duration::from_secs(1);
    let mut scheduler = Scheduler::new(config);
    let now = Instant::now();
    for name in ["a", "b", "c"] {
        scheduler.enqueue(&group(name), weight(1), event(name, 0, 1), now);
    }
    assert_eq!(scheduler.size().0, 2);
    assert_eq!(service(&scheduler.pop().unwrap()), "a");
    scheduler.enqueue(&group("c"), weight(1), event("c", 1, 1), now);
    assert_eq!(scheduler.size().0, 2);
    scheduler.enqueue(
        &group("d"),
        weight(1),
        event("d", 0, 1),
        now + Duration::from_secs(1),
    );
    assert_eq!(scheduler.size().0, 1);
    assert_eq!(service(&scheduler.pop().unwrap()), "d");
}

#[test]
fn expiration_removes_only_old_events_and_reclaims_current_turn() {
    let mut config = config();
    config.max_age_secs = Duration::from_secs(1);
    let mut scheduler = Scheduler::new(config);
    let now = Instant::now();
    for id in 0..2 {
        scheduler.enqueue(&group("a"), weight(3), event("a", id, 3), now);
    }
    scheduler.enqueue(&group("b"), weight(1), event("b", 0, 1), now);
    scheduler.pop().unwrap();
    scheduler.enqueue(
        &group("b"),
        weight(1),
        event("b", 1, 1),
        now + Duration::from_millis(500),
    );
    scheduler.expire(now + Duration::from_secs(1));
    assert_eq!(scheduler.size().1, 1);
    let event = scheduler.pop().unwrap();
    assert_eq!(service(&event), "b");
    assert_eq!(event.as_log().get(event_path!("id")), Some(&Value::from(1)));
    assert_eq!(scheduler.size(), (0, 0, 0));
    assert!(scheduler.next_expiration().is_none());
}

#[test]
fn sustained_churn_does_not_retain_empty_queue_or_timer_state() {
    let mut config = config();
    config.max_age_secs = Duration::from_millis(1);
    let mut scheduler = Scheduler::new(config);
    let now = Instant::now();
    for id in 0..2000 {
        scheduler.enqueue(
            &group(&id.to_string()),
            weight(1),
            event("a", id, 1),
            now + Duration::from_millis(u64::try_from(id).unwrap()),
        );
        assert_eq!(scheduler.size().0, 1);
    }
    assert!(scheduler.pop().is_some());
    assert!(scheduler.pop().is_none());
    assert_eq!(scheduler.size(), (0, 0, 0));
    assert!(scheduler.next_expiration().is_none());
}

#[test]
fn limiter_enforces_burst_and_does_not_bank_unbounded_idle_credit() {
    let mut config = config();
    config.rate_per_sec = weight(10);
    config.burst = Some(weight(2));
    let now = Instant::now();
    let mut gate = RateGate::new(&config, now);
    for _ in 0..2 {
        assert!(gate.deadline() <= now);
        gate.record(now);
    }
    assert_eq!(gate.deadline(), now + Duration::from_millis(100));
    let later = now + Duration::from_secs(100);
    for _ in 0..2 {
        assert!(gate.deadline() <= later);
        gate.record(later);
    }
    assert_eq!(gate.deadline(), later + Duration::from_millis(100));
}

#[test]
fn high_rates_default_to_a_small_timer_sized_burst() {
    let mut config = FairThrottleConfig::default();
    assert_eq!(config.burst_size(), 1);
    config.rate_per_sec = weight(10000);
    assert_eq!(config.burst_size(), 10);
}

#[tokio::test(start_paused = true)]
async fn transform_fairly_schedules_permits_while_consuming_input_and_drains_on_close() {
    let mut config = config();
    config.rate_per_sec = weight(4);
    let transform = Box::new(FairThrottle::new(config));
    // A leads the input stream, but cannot monopolize permits while B is waiting.
    let input = (0..20)
        .map(|id| event("a", id, 1))
        .chain((0..20).map(|id| event("b", id, 3)));
    let mut output = transform.transform(Box::pin(stream::iter(input)));
    let start = Instant::now();
    assert_eq!(service(&output.next().await.unwrap()), "a");
    assert!(matches!(futures::poll!(output.next()), Poll::Pending));
    for expected in ["a", "b", "b", "b", "a", "b", "b", "b"] {
        tokio::time::advance(Duration::from_millis(250)).await;
        assert_eq!(service(&output.next().await.unwrap()), expected);
    }
    assert_eq!(Instant::now() - start, Duration::from_secs(2));
    let remaining = output.collect::<Vec<_>>().await;
    assert_eq!(remaining.len(), 31);
}

#[tokio::test(start_paused = true)]
async fn expired_events_wake_and_release_finalizers_before_next_permit() {
    let mut config = config();
    config.rate_per_sec = weight(1);
    config.max_age_secs = Duration::from_millis(100);
    let (batch, receiver) = BatchNotifier::new_with_receiver();
    let input = vec![
        event("a", 0, 1),
        event("b", 0, 1).with_batch_notifier(&batch),
    ];
    drop(batch);
    let mut output = Box::new(FairThrottle::new(config)).transform(Box::pin(stream::iter(input)));
    assert!(output.next().await.is_some());
    assert!(matches!(futures::poll!(output.next()), Poll::Pending));
    tokio::time::advance(Duration::from_millis(100)).await;
    assert!(output.next().await.is_none());
    assert_eq!(receiver.await, BatchStatus::Delivered);
}

#[tokio::test(start_paused = true)]
async fn cancellation_releases_queued_events() {
    let (batch, mut receiver) = BatchNotifier::new_with_receiver();
    let input = vec![
        event("a", 0, 1),
        event("b", 0, 1).with_batch_notifier(&batch),
    ];
    drop(batch);
    let mut output = Box::new(FairThrottle::new(config())).transform(Box::pin(stream::iter(input)));
    output.next().await.unwrap();
    assert!(matches!(futures::poll!(output.next()), Poll::Pending));
    assert!(matches!(futures::poll!(&mut receiver), Poll::Pending));
    drop(output);
    assert_eq!(receiver.await, BatchStatus::Errored);
}

#[tokio::test]
async fn emits_internal_events() {
    use crate::{
        test_util::components::assert_transform_compliance, transforms::test::create_topology,
    };
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;
    Box::pin(assert_transform_compliance(async {
        let (tx, rx) = mpsc::channel(1);
        let (topology, mut output) = create_topology(ReceiverStream::new(rx), config()).await;
        tx.send(event("a", 0, 1)).await.unwrap();
        assert!(output.recv().await.is_some());
        drop(tx);
        topology.stop().await;
        assert!(output.recv().await.is_none());
    }))
    .await;
}

#[tokio::test]
async fn invalid_config_is_rejected_by_build() {
    let mut config = config();
    config.max_age_secs = Duration::ZERO;
    assert!(config.build(&TransformContext::default()).await.is_err());
}
