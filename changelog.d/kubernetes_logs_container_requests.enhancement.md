The `kubernetes_logs` source now includes the logging container's CPU request in
`kubernetes.container_cpu_request_millicores` and memory request in
`kubernetes.container_memory_request_bytes`. The fields can be renamed or disabled
through `pod_annotation_fields` and are omitted when the requests are unavailable.

authors: jonahgetz
