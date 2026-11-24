package metadata

generated: components: transforms: fair_throttle: configuration: {
	burst: {
		description: """
			Maximum burst of events allowed by the shared rate limiter.

			Defaults to one millisecond of output capacity, rounded up, to accommodate timer
			granularity. Set this to `1` to disable bursts, at the cost of lower throughput at high
			rates. This must not exceed `rate_per_sec`. The rate limit permits at most
			`burst + rate_per_sec * elapsed_seconds` events, rather than a strict rolling-second cap.
			"""
		required: false
		type: uint: {}
	}
	group_by: {
		description: """
			The value used to group events into independently queued services.

			Use a stable service identity, such as namespace and workload, rather than a pod or request
			identifier. Missing or unrenderable values share a fallback group. If omitted, all events
			share one group.
			"""
		required: false
		type: string: {
			examples: ["{{ service }}"]
			syntax: "template"
		}
	}
	internal_metrics: {
		description: "Configuration of internal metrics for the Fair Throttle transform."
		required:    false
		type: object: options: include_group_tag: {
			description: """
				Include a `group` tag on queue admission, emission, and discard counters.

				Missing or unrenderable groups use `None`. Only enable this for a bounded set of group
				values: limiting concurrent queues does not limit metric cardinality over time.
				"""
			required: false
			type: bool: default: false
		}
	}
	max_age_secs: {
		description: """
			Maximum time an event may wait in a service queue, in seconds.

			Expired events are intentionally dropped. Queues drain at the configured rate when input
			closes, with this age limit still enforced. Queues are held in memory and are not persisted
			across restarts. Downstream backpressure can pause input and expiration processing; expired
			events are discarded before output resumes.
			"""
		required: false
		type: float: {
			default: 1.0
			unit:    "seconds"
		}
	}
	max_bytes_per_group: {
		description: """
			Maximum estimated in-memory event bytes plus rendered key bytes per group.

			Events that would exceed this limit are dropped, including individual oversized events.
			Total retained event and key bytes are bounded by `max_groups * max_bytes_per_group`.
			Queue and allocator bookkeeping consume additional memory bounded by the group and event
			limits. This is not a serialized-byte rate limit.
			"""
		required: false
		type: uint: {
			default: 262144
			unit:    "bytes"
		}
	}
	max_events_per_group: {
		description: """
			Maximum number of queued events per group.

			When a group's queue is full, newly arriving events for that group are intentionally
			dropped. Smaller queues reduce memory and latency but tolerate smaller input bursts.
			"""
		required: false
		type: uint: default: 16
	}
	max_groups: {
		description: """
			Maximum number of simultaneously queued groups.

			Events for additional groups are dropped until an existing queue empties or expires.
			Each admitted group has its own event and byte allowance, so a busy group cannot consume
			another group's queue capacity. Choose a limit that accommodates all expected services.
			"""
		required: false
		type: uint: default: 128
	}
	rate_per_sec: {
		description: """
			The sustained number of log events allowed per second across all groups.

			Each event costs one unit, regardless of its size. The limit applies at this transform's
			output; downstream buffering and fanout can change the timing and number of exported copies.
			"""
		required: true
		type: uint: {}
	}
	weight: {
		description: """
			Default scheduling weight, also used when `weight_field` is missing or invalid.

			Larger weights receive proportionally more output capacity when groups remain backlogged.
			Unused capacity is available to other groups. Weights apply to retained event counts.
			"""
		required: false
		type: uint: default: 1
	}
	weight_field: {
		description: """
			The event field whose positive integer value supplies its group's scheduling weight.

			Accepts integers or strings containing a positive 32-bit integer. Floats, zero, negative,
			missing, and invalid values fall back to `weight`, following `sample.rate_field` semantics.
			The latest observed weight for a group applies at the start of its next scheduling turn,
			including when the observed event is dropped because the queue is full. Updating a weight
			does not replenish the current turn's credit. Prefer a consistent weight on all events
			belonging to a service. Larger weights can increase other groups' waiting time.
			"""
		required: false
		type: string: examples: ["log_weight"]
	}
}
