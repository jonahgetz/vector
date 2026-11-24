Add the `fair_throttle` log transform to share one rate limit across services using
weighted round robin. Configure service identity with `group_by` and read service
weights from `weight_field`, with `weight` as a fallback. Per-service event and byte
limits, a group limit, and queue expiration bound buffering during overload.

authors: jonahgetz
