The `kubernetes_logs` source includes the logging container's CPU request in
millicores and memory request in bytes as metadata in the Vector log namespace.
In the legacy log namespace, these fields are disabled by default. Enable them with
nonempty `pod_annotation_fields.container_cpu_request_millicores` and
`pod_annotation_fields.container_memory_request_bytes` paths.

authors: jonahgetz
