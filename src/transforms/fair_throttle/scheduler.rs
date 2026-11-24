use std::{
    collections::{BTreeSet, HashMap, VecDeque},
    num::NonZeroU32,
    sync::Arc,
    time::Duration,
};

use tokio::time::Instant;
use vector_lib::{ByteSizeOf, internal_event::emit};

use super::config::FairThrottleConfig;
use crate::{
    event::{Event, EventStatus},
    internal_events::{
        FairThrottleEventQueued, FairThrottleEventSent, FairThrottleEventsDropped,
        FairThrottleQueueState,
    },
};

pub(super) type Group = Option<Arc<str>>;

struct QueuedEvent {
    event: Event,
    bytes: usize,
    expires: Instant,
}

struct Queue {
    events: VecDeque<QueuedEvent>,
    bytes: usize,
    weight: NonZeroU32,
}

/// Unit-cost DRR. A turn's credit survives rate-limit waits and weight updates.
/// Only nonempty queues are retained. There is exactly one expiry entry per queue.
pub(super) struct Scheduler {
    queues: HashMap<Group, Queue>,
    active: VecDeque<Group>,
    current: Option<Group>,
    remaining: u32,
    expirations: BTreeSet<(Instant, Group)>,
    config: FairThrottleConfig,
    events: usize,
    bytes: usize,
}

impl Scheduler {
    pub(super) fn new(config: FairThrottleConfig) -> Self {
        let scheduler = Self {
            queues: HashMap::new(),
            active: VecDeque::new(),
            current: None,
            remaining: 0,
            expirations: BTreeSet::new(),
            config,
            events: 0,
            bytes: 0,
        };
        scheduler.report_size();
        scheduler
    }

    pub(super) fn is_empty(&self) -> bool {
        self.queues.is_empty()
    }

    pub(super) fn next_expiration(&self) -> Option<Instant> {
        self.expirations.first().map(|(deadline, _)| *deadline)
    }

    fn metric_group(&self, group: &Group) -> Option<String> {
        self.config
            .internal_metrics
            .include_group_tag
            .then(|| group.as_deref().unwrap_or("None").to_owned())
    }

    fn dropped(&self, group: &Group, count: usize, reason: &'static str, intentional: bool) {
        emit(FairThrottleEventsDropped {
            group: self.metric_group(group),
            count,
            reason,
            intentional,
        });
    }

    fn report_size(&self) {
        emit(FairThrottleQueueState {
            events: self.events,
            bytes: self.bytes,
        });
    }

    pub(super) fn enqueue(
        &mut self,
        group: &Group,
        weight: NonZeroU32,
        event: Event,
        now: Instant,
    ) {
        // Expiration also runs while input is continuously ready, reclaiming admission capacity.
        self.expire(now);
        let event_bytes = event.size_of();
        let key_bytes = group.as_deref().map_or(0, str::len);
        if let Some(queue) = self.queues.get_mut(group) {
            queue.weight = weight;
            if queue.events.len() >= self.config.max_events_per_group.get() {
                self.dropped(group, 1, "Service event queue is full.", true);
                return;
            }
            if event_bytes > self.config.max_bytes_per_group.get() - queue.bytes {
                self.dropped(group, 1, "Service byte queue is full.", true);
                return;
            }
        } else {
            if self.queues.len() >= self.config.max_groups.get() {
                self.dropped(group, 1, "Maximum number of service queues reached.", true);
                return;
            }
            if key_bytes > self.config.max_bytes_per_group.get()
                || event_bytes > self.config.max_bytes_per_group.get() - key_bytes
            {
                self.dropped(
                    group,
                    1,
                    "Event and service key exceed the byte queue limit.",
                    true,
                );
                return;
            }
            self.queues.insert(
                group.clone(),
                Queue {
                    events: VecDeque::new(),
                    bytes: key_bytes,
                    weight,
                },
            );
            self.active.push_back(group.clone());
            self.expirations
                .insert((now + self.config.max_age_secs, group.clone()));
            self.bytes += key_bytes;
        }
        let queue = self
            .queues
            .get_mut(group)
            .expect("queue exists after admission");
        queue.events.push_back(QueuedEvent {
            event,
            bytes: event_bytes,
            expires: now + self.config.max_age_secs,
        });
        queue.bytes += event_bytes;
        self.bytes += event_bytes;
        self.events += 1;
        emit(FairThrottleEventQueued {
            group: self.metric_group(group),
        });
        self.report_size();
    }

    pub(super) fn pop(&mut self) -> Option<Event> {
        if self.current.is_none() {
            let group = self.active.pop_front()?;
            self.remaining = self.queues[&group].weight.get();
            self.current = Some(group);
        }
        let group = self.current.as_ref().expect("current turn exists").clone();
        let queue = self.queues.get_mut(&group).expect("active queue exists");
        let queued = queue.events.pop_front().expect("active queue is nonempty");
        self.expirations.remove(&(queued.expires, group.clone()));
        self.events -= 1;
        self.bytes -= queued.bytes;
        queue.bytes -= queued.bytes;
        self.remaining -= 1;
        if let Some(front) = queue.events.front() {
            self.expirations.insert((front.expires, group.clone()));
            if self.remaining == 0 {
                self.active.push_back(group.clone());
                self.current = None;
            }
        } else {
            self.bytes -= queue.bytes;
            self.queues.remove(&group);
            self.current = None;
            self.remaining = 0;
        }
        emit(FairThrottleEventSent {
            group: self.metric_group(&group),
        });
        self.report_size();
        Some(queued.event)
    }

    pub(super) fn expire(&mut self, now: Instant) {
        let mut changed = false;
        let mut removed_queue = false;
        while self
            .next_expiration()
            .is_some_and(|deadline| deadline <= now)
        {
            let (_, group) = self.expirations.pop_first().expect("expiry exists");
            let queue = self.queues.get_mut(&group).expect("expiring queue exists");
            let mut count = 0;
            while queue
                .events
                .front()
                .is_some_and(|event| event.expires <= now)
            {
                let event = queue.events.pop_front().expect("expired event exists");
                queue.bytes -= event.bytes;
                self.bytes -= event.bytes;
                self.events -= 1;
                count += 1;
            }
            if let Some(front) = queue.events.front() {
                self.expirations.insert((front.expires, group.clone()));
            } else {
                self.bytes -= queue.bytes;
                self.queues.remove(&group);
                if self.current.as_ref() == Some(&group) {
                    self.current = None;
                    self.remaining = 0;
                }
                removed_queue = true;
            }
            self.dropped(
                &group,
                count,
                "Service queue event exceeded its maximum age.",
                true,
            );
            changed = true;
        }
        if removed_queue {
            // Compact once per expiration sweep, avoiding stale entries under group churn.
            self.active.retain(|group| self.queues.contains_key(group));
        }
        if changed {
            self.report_size();
        }
    }

    #[cfg(test)]
    pub(super) fn size(&self) -> (usize, usize, usize) {
        (self.queues.len(), self.events, self.bytes)
    }
}

impl Drop for Scheduler {
    fn drop(&mut self) {
        for (group, queue) in &self.queues {
            for event in &queue.events {
                event.event.metadata().update_status(EventStatus::Errored);
            }
            self.dropped(
                group,
                queue.events.len(),
                "Transform stopped with queued events.",
                false,
            );
        }
        emit(FairThrottleQueueState {
            events: 0,
            bytes: 0,
        });
    }
}

/// Shared GCRA rate gate using Tokio's clock, including under paused-time tests.
pub(super) struct RateGate {
    next: Instant,
    period: Duration,
    tolerance: Duration,
}

impl RateGate {
    pub(super) fn new(config: &FairThrottleConfig, now: Instant) -> Self {
        // Round up so nanosecond quantization cannot increase the configured rate.
        let period =
            Duration::from_nanos(1_000_000_000_u64.div_ceil(u64::from(config.rate_per_sec.get())));
        Self {
            next: now,
            period,
            tolerance: period * (config.burst_size() - 1),
        }
    }

    pub(super) fn deadline(&self) -> Instant {
        self.next.checked_sub(self.tolerance).unwrap_or(self.next)
    }

    pub(super) fn record(&mut self, now: Instant) {
        self.next = self.next.max(now) + self.period;
    }
}
