The `kubernetes_logs` source can optionally enrich logs with the logging container's
CPU request in millicores and memory request in bytes. Both fields are disabled by
default. Set `pod_annotation_fields.container_cpu_request_millicores` or
`pod_annotation_fields.container_memory_request_bytes` to a nonempty event path to
enable each field independently. Omitting a setting or setting it to `""` disables
that field in both log namespaces. In the Vector log namespace, these settings
enable the fixed `%kubernetes_logs.container_cpu_request_millicores` and
`%kubernetes_logs.container_memory_request_bytes` metadata paths without renaming
them. Missing or unrepresentable requests are omitted; explicit zeroes are retained.

authors: jonahgetz
