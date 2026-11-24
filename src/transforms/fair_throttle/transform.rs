use std::{num::NonZeroU32, pin::Pin};

use async_stream::stream;
use futures::{Stream, StreamExt};
use tokio::time::{Instant, sleep_until};
use vector_lib::internal_event::emit;

use super::{
    config::FairThrottleConfig,
    scheduler::{Group, RateGate, Scheduler},
};
use crate::{
    event::{Event, Value},
    internal_events::TemplateRenderingError,
    transforms::TaskTransform,
};

pub(super) struct FairThrottle {
    config: FairThrottleConfig,
}

impl FairThrottle {
    pub(super) const fn new(config: FairThrottleConfig) -> Self {
        Self { config }
    }

    pub(super) fn event_weight(&self, event: &Event) -> NonZeroU32 {
        self.config
            .weight_field
            .as_ref()
            .and_then(|path| event.as_log().parse_path_and_get_value(path).ok().flatten())
            .and_then(|value| match value {
                Value::Integer(value) => u32::try_from(*value).ok().and_then(NonZeroU32::new),
                Value::Bytes(bytes) => std::str::from_utf8(bytes).ok()?.parse().ok(),
                Value::String(value) => value.parse().ok(),
                _ => None,
            })
            .unwrap_or(self.config.weight)
    }

    pub(super) fn group(&self, event: &Event) -> Group {
        self.config.group_by.as_ref().and_then(|template| {
            template
                .render_string(event)
                .map_err(|error| {
                    emit(TemplateRenderingError {
                        error,
                        field: Some("group_by"),
                        drop_event: false,
                    });
                })
                .ok()
                .map(Into::into)
        })
    }
}

impl TaskTransform<Event> for FairThrottle {
    fn transform(
        self: Box<Self>,
        mut input: Pin<Box<dyn Stream<Item = Event> + Send>>,
    ) -> Pin<Box<dyn Stream<Item = Event> + Send>> {
        Box::pin(stream! {
            let mut gate = RateGate::new(&self.config, Instant::now());
            let mut scheduler = Scheduler::new(self.config.clone());
            let mut done = false;
            loop {
                // Ready input streams and long runs of dropped events must not monopolize a worker.
                tokio::task::consume_budget().await;
                let now = Instant::now();
                scheduler.expire(now);
                if done && scheduler.is_empty() { break; }

                if !scheduler.is_empty() && gate.deadline() <= now {
                    if let Some(event) = scheduler.pop() {
                        gate.record(now);
                        yield event;
                    }
                    continue;
                }

                let deadline = scheduler.next_expiration().map(|expires| expires.min(gate.deadline()));
                tokio::select! {
                    biased;
                    // Keep classifying input while waiting for capacity. Expiration also wakes
                    // the task when the global rate is too low to emit before a queue ages out.
                    () = sleep_until(deadline.unwrap_or(now)), if deadline.is_some() => {},
                    event = input.next(), if !done => {
                        match event {
                            Some(event) => {
                                let group = self.group(&event);
                                let weight = self.event_weight(&event);
                                scheduler.enqueue(&group, weight, event, Instant::now());
                            }
                            None => done = true,
                        }
                    }
                }
            }
        })
    }
}
