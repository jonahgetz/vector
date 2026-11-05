The `kubernetes_logs` source can include the logging container's CPU request in
millicores and memory request in bytes. Both fields are disabled by default. Enable
them with nonempty `pod_annotation_fields.container_cpu_request_millicores` and
`pod_annotation_fields.container_memory_request_bytes` paths.

authors: jonahgetz
