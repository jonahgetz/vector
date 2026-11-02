use std::{hash::Hash, num::NonZeroU32, pin::Pin, time::Duration};

use async_stream::stream;
use futures::{Stream, StreamExt};
use governor::{Quota, clock};
use metrics::Counter;
use snafu::Snafu;

use super::{
    config::{ThrottleConfig, ThrottleInternalMetricsConfig},
    rate_limiter::RateLimiterRunner,
};
use crate::{
    conditions::Condition,
    config::TransformContext,
    event::{Event, Value},
    internal_events::{TemplateRenderingError, ThrottleEventDiscarded},
    template::UnconfinedTemplate,
    transforms::TaskTransform,
};

#[derive(Clone)]
pub struct Throttle<C: clock::Clock<Instant = I>, I: clock::Reference> {
    pub quota: Quota,
    pub flush_keys_interval: Duration,
    key_field: Option<UnconfinedTemplate>,
    threshold_field: Option<String>,
    exclude: Option<Condition>,
    pub clock: C,
    internal_metrics: ThrottleInternalMetricsConfig,
    pub cpu_ns: Option<Counter>,
}

impl<C, I> Throttle<C, I>
where
    C: clock::Clock<Instant = I> + Clone + Send + Sync + 'static,
    I: clock::Reference,
{
    ///
    /// # Errors
    ///
    /// Returns an error if the exclusion condition cannot be built.
    pub fn new(
        config: &ThrottleConfig,
        context: &TransformContext,
        clock: C,
    ) -> crate::Result<Self> {
        let flush_keys_interval = config.window_secs;

        let Some(threshold) = NonZeroU32::new(config.threshold) else {
            return Err(Box::new(ConfigError::NonZero));
        };

        let quota = match Quota::with_period(Duration::from_secs_f64(
            flush_keys_interval.as_secs_f64() / f64::from(threshold.get()),
        )) {
            Some(quota) => quota.allow_burst(threshold),
            None => return Err(Box::new(ConfigError::NonZero)),
        };
        let exclude = config
            .exclude
            .as_ref()
            .map(|condition| condition.build(&context.enrichment_tables, &context.metrics_storage))
            .transpose()?;

        Ok(Self {
            quota,
            clock,
            flush_keys_interval,
            key_field: config.key_field.clone(),
            threshold_field: config.threshold_field.clone(),
            exclude,
            internal_metrics: config.internal_metrics.clone(),
            cpu_ns: context.cpu_ns.clone(),
        })
    }

    #[must_use]
    pub fn start_rate_limiter<K>(&self) -> RateLimiterRunner<K, C>
    where
        K: Hash + Eq + Clone + Send + Sync + 'static,
    {
        RateLimiterRunner::start(self)
    }

    fn event_quota(&self, event: &Event) -> Option<Quota> {
        let field = self.threshold_field.as_deref()?;
        let value = event.as_log().parse_path_and_get_value(field).ok()??;
        let threshold = match value {
            Value::Integer(value) => u32::try_from(*value).ok().and_then(NonZeroU32::new),
            Value::Bytes(bytes) => std::str::from_utf8(bytes).ok()?.parse::<NonZeroU32>().ok(),
            Value::String(string) => string.parse::<NonZeroU32>().ok(),
            _ => None,
        }?;

        Quota::with_period(Duration::from_secs_f64(
            self.flush_keys_interval.as_secs_f64() / f64::from(threshold.get()),
        ))
        .map(|quota| quota.allow_burst(threshold))
    }

    pub fn emit_event_discarded(&self, event: &Event, key: String) {
        let metric_key = self
            .internal_metrics
            .key_field
            .as_ref()
            .filter(|_| {
                self.internal_metrics.emit_events_discarded_per_key
                    || self.internal_metrics.include_group_tag
            })
            .map(|template| {
                template.render_string(event).unwrap_or_else(|error| {
                    emit!(TemplateRenderingError {
                        error,
                        field: Some("internal_metrics.key_field"),
                        drop_event: false,
                    });
                    "None".to_owned()
                })
            });
        emit!(ThrottleEventDiscarded {
            key,
            metric_key,
            emit_events_discarded_per_key: self.internal_metrics.emit_events_discarded_per_key,
            include_group_tag: self.internal_metrics.include_group_tag,
        });
    }
}

impl<C, I> TaskTransform<Event> for Throttle<C, I>
where
    C: clock::Clock<Instant = I> + Clone + Send + Sync + 'static,
    I: clock::Reference + Send + 'static,
{
    fn transform(
        self: Box<Self>,
        mut input_rx: Pin<Box<dyn Stream<Item = Event> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = Event> + Send>>
    where
        Self: 'static,
    {
        let limiter = self.start_rate_limiter();

        Box::pin(stream! {
            while let Some(event) = input_rx.next().await {
                let (throttle, event) = match self.exclude.as_ref() {
                    Some(condition) => {
                        let (result, event) = condition.check(event);
                        (!result, event)
                    },
                    _ => (true, event)
                };
                let output = if throttle {
                    let key = self.key_field.as_ref().and_then(|t| {
                        t.render_string(&event)
                            .map_err(|error| {
                                emit!(TemplateRenderingError {
                                    error,
                                    field: Some("key_field"),
                                    drop_event: false,
                                });
                            })
                            .ok()
                    });

                    let quota = self.event_quota(&event).unwrap_or(self.quota);
                    if limiter.check_key(&key, quota) {
                        Some(event)
                    } else {
                        self.emit_event_discarded(&event, key.unwrap_or_else(|| "None".to_string()));
                        None
                    }
                } else {
                    Some(event)
                };
                if let Some(event) = output {
                    yield event;
                }
            }
        })
    }
}

#[derive(Debug, Snafu)]
pub enum ConfigError {
    #[snafu(display("`threshold`, and `window_secs` must be non-zero"))]
    NonZero,
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, task::Poll};

    use futures::SinkExt;
    use indoc::indoc;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;
    use vector_lib::metrics::Controller;
    use vrl::event_path;

    use super::*;
    use crate::{
        event::{LogEvent, MetricValue},
        test_util::components::assert_transform_compliance,
        transforms::{Transform, test::create_topology},
    };

    async fn throttle_logs(config: &ThrottleConfig, logs: Vec<LogEvent>) -> Vec<Event> {
        let throttle = Throttle::new(
            config,
            &TransformContext::default(),
            clock::FakeRelativeClock::default(),
        )
        .unwrap();

        Box::new(throttle)
            .transform(Box::pin(futures::stream::iter(
                logs.into_iter().map(Event::Log),
            )))
            .collect()
            .await
    }

    #[tokio::test]
    async fn metric_key_preserves_pod_buckets_and_combines_service_drops() {
        for (per_key, group_tag) in [(true, false), (false, true), (true, true)] {
            vector_lib::metrics::init_test();
            let controller = Controller::get().unwrap();
            controller.reset();
            let mut config: ThrottleConfig = serde_yaml::from_str(indoc! {"
                threshold: 1
                window_secs: 60
                key_field: '{{ pod }}'
                internal_metrics:
                  key_field: '{{ service }}'
            "})
            .unwrap();
            config.internal_metrics.emit_events_discarded_per_key = per_key;
            config.internal_metrics.include_group_tag = group_tag;
            let logs = [
                ("pod-a", Some("service-a")),
                ("pod-a", Some("service-a")),
                ("pod-b", Some("service-a")),
                ("pod-b", Some("service-a")),
                ("pod-a", Some("service-b")),
                ("pod-c", None),
                ("pod-c", None),
            ]
            .into_iter()
            .map(|(pod, service)| {
                let mut log = LogEvent::from("hello");
                log.insert(event_path!("pod"), pod);
                if let Some(service) = service {
                    log.insert(event_path!("service"), service);
                }
                log
            })
            .collect();
            assert_eq!(throttle_logs(&config, logs).await.len(), 3);
            let mut drops = BTreeMap::new();
            let mut groups = BTreeMap::new();
            let mut total = 0.0;
            for metric in controller.capture_metrics() {
                if let MetricValue::Counter { value } = metric.value() {
                    match metric.name() {
                        "events_discarded_total" => {
                            drops.insert(metric.tag_value("key").unwrap(), *value);
                        }
                        "component_discarded_events_total" => {
                            total += value;
                            if let Some(group) = metric.tag_value("group") {
                                groups.insert(group, *value);
                            }
                        }
                        _ => {}
                    }
                }
            }
            let expected = BTreeMap::from([
                ("None".to_owned(), 1.0),
                ("service-a".to_owned(), 2.0),
                ("service-b".to_owned(), 1.0),
            ]);
            assert_eq!(
                drops,
                if per_key {
                    expected.clone()
                } else {
                    BTreeMap::new()
                }
            );
            assert_eq!(groups, if group_tag { expected } else { BTreeMap::new() });
            assert_eq!(total, 4.0);
        }
    }

    #[tokio::test]
    async fn metric_key_defaults_to_bucket_and_respects_disabled_metrics() {
        for (per_key, group_tag) in [(true, false), (false, true), (true, true), (false, false)] {
            vector_lib::metrics::init_test();
            let controller = Controller::get().unwrap();
            controller.reset();
            let config = ThrottleConfig {
                threshold: 1,
                window_secs: Duration::from_mins(1),
                key_field: Some(UnconfinedTemplate::try_from("{{ pod }}").unwrap()),
                internal_metrics: ThrottleInternalMetricsConfig {
                    emit_events_discarded_per_key: per_key,
                    include_group_tag: group_tag,
                    key_field: if per_key || group_tag {
                        None
                    } else {
                        Some(UnconfinedTemplate::try_from("{{ missing }}").unwrap())
                    },
                },
                ..Default::default()
            };
            let mut log = LogEvent::from("hello");
            log.insert(event_path!("pod"), "pod-a");
            assert_eq!(throttle_logs(&config, vec![log; 2]).await.len(), 1);
            let counters: BTreeMap<_, _> = controller
                .capture_metrics()
                .into_iter()
                .filter_map(|metric| {
                    if let MetricValue::Counter { value } = metric.value() {
                        if metric.name() == "events_discarded_total" {
                            assert_eq!(metric.tag_value("key").as_deref(), Some("pod-a"));
                        }
                        if metric.name() == "component_discarded_events_total" {
                            assert_eq!(
                                metric.tag_value("group").as_deref(),
                                group_tag.then_some("pod-a")
                            );
                        }
                        Some((metric.name().to_owned(), *value))
                    } else {
                        None
                    }
                })
                .collect();
            assert_eq!(
                counters.get("events_discarded_total"),
                per_key.then_some(&1.0)
            );
            assert_eq!(counters.get("component_discarded_events_total"), Some(&1.0));
            assert!(!counters.contains_key("component_errors_total"));
        }
    }

    #[tokio::test]
    async fn threshold_field_overrides_static_threshold_or_falls_back() {
        let config: ThrottleConfig = serde_yaml::from_str(indoc! {"
            threshold: 2
            threshold_field: limit
            window_secs: 5
        "})
        .unwrap();

        for (value, expected) in [
            (Some(Value::Integer(1)), 1),
            (Some(Value::Integer(3)), 3),
            (Some(Value::Bytes("3".into())), 3),
            (Some(Value::String("3".into())), 3),
            (Some(Value::Integer(i64::from(u32::MAX))), 5),
            (Some(Value::Bytes(u32::MAX.to_string().into())), 5),
            (None, 2),
            (Some(Value::Null), 2),
            (Some(Value::Integer(0)), 2),
            (Some(Value::Integer(-1)), 2),
            (Some(Value::Integer(i64::from(u32::MAX) + 1)), 2),
            (Some(Value::from(3.0)), 2),
            (Some(Value::Boolean(true)), 2),
            (Some(Value::Array(Vec::new())), 2),
            (Some(Value::Object(BTreeMap::new())), 2),
            (Some(Value::Bytes("0".into())), 2),
            (Some(Value::Bytes("-1".into())), 2),
            (Some(Value::Bytes("3.0".into())), 2),
            (Some(Value::Bytes("4294967296".into())), 2),
            (Some(Value::Bytes("invalid".into())), 2),
            (Some(Value::Bytes(vec![0xff].into())), 2),
        ] {
            let mut log = LogEvent::from("hello");
            if let Some(value) = value.clone() {
                log.insert(event_path!("limit"), value);
            }
            let output = throttle_logs(&config, vec![log.clone(); 5]).await;
            assert_eq!(output, vec![Event::Log(log); expected], "value: {value:?}");
        }
    }

    #[tokio::test]
    async fn threshold_field_supports_event_and_metadata_paths() {
        for path in [
            "limits[0].threshold",
            ".limits[0].threshold",
            "\"rate.limit\"",
            "%limit",
        ] {
            let config = ThrottleConfig {
                threshold: 1,
                threshold_field: Some(path.to_string()),
                window_secs: Duration::from_secs(5),
                ..Default::default()
            };
            let mut log = LogEvent::from("hello");
            log.insert(&vrl::path::parse_target_path(path).unwrap(), 2);

            assert_eq!(
                throttle_logs(&config, vec![log.clone(); 3]).await,
                vec![Event::Log(log); 2],
                "path: {path}",
            );
        }
    }

    #[tokio::test]
    async fn threshold_field_shares_usage_when_threshold_changes() {
        let config = ThrottleConfig {
            threshold: 2,
            threshold_field: Some("limit".to_string()),
            window_secs: Duration::from_secs(8),
            ..Default::default()
        };

        for thresholds in [[2, 4, 4, 4], [4, 4, 2, 4], [4, 4, 0, 4]] {
            let logs: Vec<_> = thresholds
                .into_iter()
                .map(|threshold| {
                    let mut log = LogEvent::from("hello");
                    log.insert(event_path!("limit"), threshold);
                    log
                })
                .collect();
            let expected: Vec<_> = logs[..3].iter().cloned().map(Event::Log).collect();

            assert_eq!(throttle_logs(&config, logs).await, expected);
        }
    }

    #[tokio::test]
    async fn threshold_field_rate_limits_buckets_independently() {
        let config: ThrottleConfig = serde_yaml::from_str(indoc! {r#"
            threshold: 2
            threshold_field: limit
            window_secs: 5
            key_field: "{{ bucket }}"
        "#})
        .unwrap();
        let mut log_a = LogEvent::from("a");
        log_a.insert(event_path!("bucket"), "a");
        log_a.insert(event_path!("limit"), 1);
        let mut log_b = LogEvent::from("b");
        log_b.insert(event_path!("bucket"), "b");
        log_b.insert(event_path!("limit"), 3);
        let logs = [log_a.clone(), log_b.clone()]
            .into_iter()
            .cycle()
            .take(8)
            .collect();

        assert_eq!(
            throttle_logs(&config, logs).await,
            vec![
                log_a.into(),
                log_b.clone().into(),
                log_b.clone().into(),
                log_b.into()
            ],
        );
    }

    #[tokio::test]
    async fn threshold_field_replenishes_capacity() {
        let config = ThrottleConfig {
            threshold: 1,
            threshold_field: Some("limit".to_string()),
            window_secs: Duration::from_secs(8),
            ..Default::default()
        };
        let clock = clock::FakeRelativeClock::default();
        let throttle = Throttle::new(&config, &TransformContext::default(), clock.clone()).unwrap();
        let events = [
            (0, 4),
            (0, 4),
            (0, 4),
            (0, 4),
            (0, 4),
            (1, 4),
            (1, 4),
            (1, 2),
            (3, 2),
        ]
        .into_iter()
        .enumerate()
        .map(move |(index, (advance, threshold))| {
            clock.advance(Duration::from_secs(advance));
            let mut log = LogEvent::from(index.to_string());
            log.insert(event_path!("limit"), threshold);
            Event::Log(log)
        });
        let output: Vec<_> = Box::new(throttle)
            .transform(Box::pin(futures::stream::iter(events)))
            .map(|event| event.as_log()["message"].clone())
            .collect()
            .await;

        assert_eq!(output, ["0", "1", "2", "3", "6", "8"].map(Value::from));
    }

    #[tokio::test]
    async fn threshold_field_preserves_exclusions() {
        let config: ThrottleConfig = serde_yaml::from_str(indoc! {"
            threshold: 2
            threshold_field: limit
            window_secs: 5
            exclude: exists(.special)
        "})
        .unwrap();
        let mut log = LogEvent::from("hello");
        log.insert(event_path!("limit"), 1);
        let mut excluded = log.clone();
        excluded.insert(event_path!("special"), true);

        assert_eq!(
            throttle_logs(
                &config,
                vec![excluded.clone(), log.clone(), excluded.clone(), log.clone()]
            )
            .await,
            vec![excluded.clone().into(), log.into(), excluded.into()],
        );
    }

    #[tokio::test]
    async fn threshold_field_falls_back_when_replenishment_interval_rounds_to_zero() {
        let config = ThrottleConfig {
            threshold: 1,
            threshold_field: Some("limit".to_string()),
            window_secs: Duration::from_nanos(1),
            ..Default::default()
        };
        let mut log = LogEvent::from("hello");
        log.insert(event_path!("limit"), 3);

        assert_eq!(
            throttle_logs(&config, vec![log.clone(); 2]).await,
            vec![Event::Log(log)],
        );
    }

    #[tokio::test]
    async fn throttle_events() {
        let clock = clock::FakeRelativeClock::default();
        let config = serde_yaml::from_str::<ThrottleConfig>(indoc! {"
            threshold: 2
            window_secs: 5
        "})
        .unwrap();

        let throttle = Throttle::new(&config, &TransformContext::default(), clock.clone())
            .map(Transform::event_task)
            .unwrap();

        let throttle = throttle.into_task();

        let (mut tx, rx) = futures::channel::mpsc::channel(10);
        let mut out_stream = throttle.transform_events(Box::pin(rx));

        // tokio interval is always immediately ready, so we poll once to make sure
        // we trip it/set the interval in the future
        assert_eq!(Poll::Pending, futures::poll!(out_stream.next()));

        tx.send(LogEvent::default().into()).await.unwrap();
        tx.send(LogEvent::default().into()).await.unwrap();

        let mut count = 0_u8;
        while count < 2 {
            match out_stream.next().await {
                Some(_event) => {
                    count += 1;
                }
                _ => {
                    panic!("Unexpectedly received None in output stream");
                }
            }
        }
        assert_eq!(2, count);

        clock.advance(Duration::from_secs(2));

        tx.send(LogEvent::default().into()).await.unwrap();

        // We should be back to pending, having the second event dropped
        assert_eq!(Poll::Pending, futures::poll!(out_stream.next()));

        clock.advance(Duration::from_secs(3));

        tx.send(LogEvent::default().into()).await.unwrap();

        // The rate limiter should now be refreshed and allow an additional event through
        match out_stream.next().await {
            Some(_event) => {}
            _ => {
                panic!("Unexpectedly received None in output stream");
            }
        }

        // We should be back to pending, having nothing waiting for us
        assert_eq!(Poll::Pending, futures::poll!(out_stream.next()));

        tx.disconnect();

        // And still nothing there
        assert_eq!(Poll::Ready(None), futures::poll!(out_stream.next()));
    }

    #[tokio::test]
    async fn throttle_exclude() {
        let clock = clock::FakeRelativeClock::default();
        let config = serde_yaml::from_str::<ThrottleConfig>(indoc! {"
            threshold: 2
            window_secs: 5
            exclude: \"exists(.special)\"
        "})
        .unwrap();

        let throttle = Throttle::new(&config, &TransformContext::default(), clock.clone())
            .map(Transform::event_task)
            .unwrap();

        let throttle = throttle.into_task();

        let (mut tx, rx) = futures::channel::mpsc::channel(10);
        let mut out_stream = throttle.transform_events(Box::pin(rx));

        // tokio interval is always immediately ready, so we poll once to make sure
        // we trip it/set the interval in the future
        assert_eq!(Poll::Pending, futures::poll!(out_stream.next()));

        tx.send(LogEvent::default().into()).await.unwrap();
        tx.send(LogEvent::default().into()).await.unwrap();

        let mut count = 0_u8;
        while count < 2 {
            match out_stream.next().await {
                Some(_event) => {
                    count += 1;
                }
                _ => {
                    panic!("Unexpectedly received None in output stream");
                }
            }
        }
        assert_eq!(2, count);

        clock.advance(Duration::from_secs(2));

        tx.send(LogEvent::default().into()).await.unwrap();

        // We should be back to pending, having the second event dropped
        assert_eq!(Poll::Pending, futures::poll!(out_stream.next()));

        let mut special_log = LogEvent::default();
        special_log.insert(event_path!("special"), "true");
        tx.send(special_log.into()).await.unwrap();
        // The rate limiter should allow this log through regardless of current limit
        match out_stream.next().await {
            Some(_event) => {}
            _ => {
                panic!("Unexpectedly received None in output stream");
            }
        }

        clock.advance(Duration::from_secs(3));

        tx.send(LogEvent::default().into()).await.unwrap();

        // The rate limiter should now be refreshed and allow an additional event through
        match out_stream.next().await {
            Some(_event) => {}
            _ => {
                panic!("Unexpectedly received None in output stream");
            }
        }

        // We should be back to pending, having nothing waiting for us
        assert_eq!(Poll::Pending, futures::poll!(out_stream.next()));

        tx.disconnect();

        // And still nothing there
        assert_eq!(Poll::Ready(None), futures::poll!(out_stream.next()));
    }

    #[tokio::test]
    async fn throttle_buckets() {
        let clock = clock::FakeRelativeClock::default();
        let config = serde_yaml::from_str::<ThrottleConfig>(indoc! {r#"
            threshold: 1
            window_secs: 5
            key_field: "{{ bucket }}"
        "#})
        .unwrap();

        let throttle = Throttle::new(&config, &TransformContext::default(), clock.clone())
            .map(Transform::event_task)
            .unwrap();

        let throttle = throttle.into_task();

        let (mut tx, rx) = futures::channel::mpsc::channel(10);
        let mut out_stream = throttle.transform_events(Box::pin(rx));

        // tokio interval is always immediately ready, so we poll once to make sure
        // we trip it/set the interval in the future
        assert_eq!(Poll::Pending, futures::poll!(out_stream.next()));

        let mut log_a = LogEvent::default();
        log_a.insert(event_path!("bucket"), "a");
        let mut log_b = LogEvent::default();
        log_b.insert(event_path!("bucket"), "b");
        tx.send(log_a.into()).await.unwrap();
        tx.send(log_b.into()).await.unwrap();

        let mut count = 0_u8;
        while count < 2 {
            match out_stream.next().await {
                Some(_event) => {
                    count += 1;
                }
                _ => {
                    panic!("Unexpectedly received None in output stream");
                }
            }
        }
        assert_eq!(2, count);

        // We should be back to pending, having nothing waiting for us
        assert_eq!(Poll::Pending, futures::poll!(out_stream.next()));

        tx.disconnect();

        // And still nothing there
        assert_eq!(Poll::Ready(None), futures::poll!(out_stream.next()));
    }

    #[tokio::test]
    async fn emits_internal_events() {
        Box::pin(assert_transform_compliance(async move {
            let config = ThrottleConfig {
                threshold: 1,
                threshold_field: None,
                window_secs: Duration::from_secs_f64(1.0),
                key_field: None,
                exclude: None,
                internal_metrics: ThrottleInternalMetricsConfig::default(),
            };
            let (tx, rx) = mpsc::channel(1);
            let (topology, mut out) = create_topology(ReceiverStream::new(rx), config).await;

            let log = LogEvent::from("hello world");
            tx.send(log.into()).await.unwrap();

            _ = out.recv().await;

            drop(tx);
            topology.stop().await;
            assert_eq!(out.recv().await, None);
        }))
        .await;
    }
}
