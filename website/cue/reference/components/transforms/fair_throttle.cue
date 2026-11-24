package metadata

components: transforms: fair_throttle: {
	title:       "Fair Throttle"
	description: "Shares a node-wide log rate limit between weighted service queues, dropping excess events."
	classes: {
		development:   "beta"
		egress_method: "stream"
		stateful:      true
	}
	features: filter: {}
	support: {
		requirements: []
		warnings: ["Queues are held in memory. Small queues can discard input bursts even when their average rate is below a service's share."]
	}
	configuration: generated.components.transforms.fair_throttle.configuration & {
		rate_per_sec: type: uint: examples: [10000]
	}
	input: {
		logs:    true
		metrics: null
		traces:  false
	}
	output: logs: "": description: "The unmodified input log event."
	telemetry: metrics: {
		buffer_received_events_total:  components.sources.internal_metrics.output.metrics.buffer_received_events_total
		buffer_sent_events_total:      components.sources.internal_metrics.output.metrics.buffer_sent_events_total
		buffer_discarded_events_total: components.sources.internal_metrics.output.metrics.buffer_discarded_events_total
		buffer_size_events:            components.sources.internal_metrics.output.metrics.buffer_size_events
		buffer_size_bytes:             components.sources.internal_metrics.output.metrics.buffer_size_bytes
	}
	how_it_works: {
		fairness: {
			title: "Weighted service queues"
			body: """
				The transform groups events using `group_by` and selects events with weighted deficit round robin.
				Each event costs one unit, so this is weighted round robin over event counts. A shared rate gate
				controls when selected events can leave. Services with weights 1 and 3 receive approximately 25%
				and 75% of the capacity while both queues remain backlogged. Empty queues relinquish capacity,
				and their unused credit is reset. Surviving events remain in FIFO order within each group.
				
				Use `weight_field` to read the service weight from an event field, with `weight` as the fallback.
				For example, set `group_by: "{{ service }}"`, `weight_field: log_weight`, and `weight: 1`.
				The latest observed value applies on the group's next turn, without refilling its current credit.
				Weights should be consistent within a service. Large quanta produce longer bursts of one service.
				
				The rate gate allows the configured `burst` and replenishes capacity at `rate_per_sec`. The default
				burst is one millisecond of capacity, rounded up. For example, 10,000 events per second permits a
				burst of 10 events. It does not promise a strict maximum in every rolling second. The limit applies
				at this transform's output; downstream batching and fanout can change export timing and counts.
				"""
		}
		buffering: {
			title: "Bounded buffering and overload"
			body: """
				Each service has its own event and byte allowance. New arrivals are discarded when their service's
				queue is full, and queued events expire after `max_age_secs`. Input continues to be classified
				while waiting for rate capacity. Downstream backpressure can still pause the whole transform.
				
				The defaults permit 128 active groups, 16 events per group, and 256 KiB of estimated event and
				key storage per group: at most 2,048 queued events and 32 MiB, plus queue bookkeeping. Limits
				are allocated on demand. Additional groups are dropped while `max_groups` queues are occupied;
				choose this limit above the expected number of services. Empty and expired queues release slots.
				
				Fairness only applies to logs that reach the scheduler. For Kubernetes log sources, consider
				`oldest_first: false` to avoid an older file backlog delaying other services. Group by service
				identity rather than pod identity so additional replicas do not receive additional shares.
				
				On input closure, queued events drain at the configured rate and still expire normally. State
				is not persisted across restarts. Forced cancellation discards remaining queued events.
				"""
		}
		monitoring: {
			title: "Monitoring queue admission and output"
			body: """
				Queue metrics carry `buffer_type="fair_throttle"` to distinguish them from topology buffers.
				`buffer_received_events_total` counts events admitted to service queues, `buffer_sent_events_total`
				counts emitted events, and `buffer_discarded_events_total` counts admission failures and queued
				drops with a bounded `reason` label. Drops also increment `component_discarded_events_total`.
				`buffer_size_events` and `buffer_size_bytes` describe the service queues, not upstream channels.
				Enable `internal_metrics.include_group_tag` for per-service counters only when group cardinality
				is bounded over time.
				"""
		}
	}
}
