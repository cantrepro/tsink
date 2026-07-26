# tsink-protocol

`tsink-protocol` contains the generated Prometheus remote read/write and OTLP metrics protobuf
models shared by tsink protocol adapters and optional test fixtures.

It intentionally contains no storage engine, HTTP server, authentication, cluster, async-runtime,
or transport implementation. Applications that only use the embedded `tsink` engine do not need
this crate.
