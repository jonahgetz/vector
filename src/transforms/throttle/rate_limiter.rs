use std::{hash::Hash, sync::Arc, time::Duration};

use governor::{
    Quota, clock,
    clock::Reference as _,
    nanos::Nanos,
    state::{
        StateStore as _,
        keyed::{DashMapStateStore, ShrinkableKeyedStateStore as _},
    },
};
use tokio;

use super::transform::Throttle;
use crate::cpu_time::spawn_timed;

/// A GCRA rate limiter with per-event quotas and periodic removal of inactive keys.
pub struct RateLimiterRunner<K, C>
where
    K: Hash + Eq + Clone,
    C: clock::Clock,
{
    state: Arc<DashMapStateStore<K>>,
    clock: C,
    start: C::Instant,
    flush_handle: tokio::task::JoinHandle<()>,
}

impl<K, C> RateLimiterRunner<K, C>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    C: clock::Clock + Clone + Send + Sync + 'static,
{
    pub fn start(throttle: &Throttle<C, C::Instant>) -> Self {
        let state = Arc::new(DashMapStateStore::<K>::default());
        let clock = throttle.clock.clone();
        let start = clock.now();

        let flush_keys_interval = throttle.flush_keys_interval;
        let cpu_ns = throttle.cpu_ns.clone();

        let state_clone = Arc::clone(&state);
        let clock_clone = clock.clone();
        // Hook the periodic key-flush task onto the component's CPU counter so
        // its housekeeping work is attributed to this throttle transform.
        let flush_handle = spawn_timed(
            async move {
                let mut interval = tokio::time::interval(flush_keys_interval);
                loop {
                    interval.tick().await;
                    state_clone.retain_recent(clock_clone.now().duration_since(start));
                }
            },
            cpu_ns,
        );

        Self {
            state,
            clock,
            start,
            flush_handle,
        }
    }

    pub fn check_key(&self, key: &K, quota: Quota) -> bool {
        let now = self.clock.now().duration_since(self.start);
        let interval: Nanos = quota
            .replenish_interval()
            .max(Duration::from_nanos(1))
            .into();
        let tolerance = interval * u64::from(quota.burst_size().get() - 1);

        self.state
            .measure_and_replace(key, |arrival| {
                let arrival = arrival.unwrap_or(now);
                if now < arrival.saturating_sub(tolerance) {
                    Err(())
                } else {
                    Ok(((), arrival.max(now) + interval))
                }
            })
            .is_ok()
    }
}

impl<K, C> Drop for RateLimiterRunner<K, C>
where
    K: Hash + Eq + Clone,
    C: clock::Clock,
{
    fn drop(&mut self) {
        self.flush_handle.abort();
    }
}

#[cfg(test)]
mod tests {
    use governor::RateLimiter;

    use super::*;
    use crate::{config::TransformContext, transforms::throttle::config::ThrottleConfig};

    #[tokio::test]
    async fn fixed_quotas_match_governor() {
        for threshold in [1, 2, 3, 10] {
            let clock = clock::FakeRelativeClock::default();
            let config = ThrottleConfig {
                threshold,
                window_secs: Duration::from_secs(5),
                ..Default::default()
            };
            let throttle =
                Throttle::new(&config, &TransformContext::default(), clock.clone()).unwrap();
            let limiter = throttle.start_rate_limiter();
            let reference = RateLimiter::dashmap_with_clock(throttle.quota, clock.clone());

            for advance in [
                Duration::ZERO,
                Duration::from_nanos(1),
                Duration::from_millis(500),
                Duration::from_secs(3),
                Duration::from_secs(10),
            ] {
                clock.advance(advance);
                for key in [0, 1, 0, 1, 0, 2, 0].repeat(3) {
                    assert_eq!(
                        limiter.check_key(&key, throttle.quota),
                        reference.check_key(&key).is_ok(),
                        "threshold: {threshold}, advance: {advance:?}, key: {key}",
                    );
                }
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn removes_replenished_buckets() {
        let clock = clock::FakeRelativeClock::default();
        let config = ThrottleConfig {
            threshold: 1,
            window_secs: Duration::from_secs(5),
            ..Default::default()
        };
        let throttle = Throttle::new(&config, &TransformContext::default(), clock.clone()).unwrap();
        let limiter = throttle.start_rate_limiter();
        assert!(limiter.check_key(&"bucket", throttle.quota));
        tokio::task::yield_now().await;
        assert_eq!(limiter.state.len(), 1);

        clock.advance(Duration::from_secs(6));
        tokio::time::advance(Duration::from_secs(5)).await;
        tokio::task::yield_now().await;

        assert!(limiter.state.is_empty());
        assert!(limiter.check_key(&"bucket", throttle.quota));
    }
}
