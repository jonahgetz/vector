The `throttle` transform now supports `threshold_field` to override its threshold
from each event, falling back to `threshold` when the field is missing or invalid.
It also supports `internal_metrics.key_field` to group drop metrics independently
of rate-limit buckets.

authors: jonahgetz
