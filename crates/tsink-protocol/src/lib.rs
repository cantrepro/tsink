//! Reusable generated protocol models for tsink adapters.
//!
//! This crate owns protobuf types only. It deliberately does not depend on the tsink storage
//! engine or provide HTTP, authentication, cluster, runtime, or transport behavior.

#![forbid(unsafe_code)]

/// Prometheus remote read/write protobuf models.
pub mod prometheus {
    #[allow(clippy::all, dead_code, missing_docs)]
    mod generated {
        include!(concat!(env!("OUT_DIR"), "/prometheus.rs"));
    }

    #[allow(unused_imports)]
    pub use generated::chunk;
    #[allow(unused_imports)]
    pub use generated::chunk::Encoding as ChunkEncoding;
    #[allow(unused_imports)]
    pub use generated::histogram;
    #[allow(unused_imports)]
    pub use generated::histogram::ResetHint as HistogramResetHint;
    #[allow(unused_imports)]
    pub use generated::label_matcher;
    pub use generated::label_matcher::Type as MatcherType;
    #[allow(unused_imports)]
    pub use generated::metric_metadata;
    pub use generated::metric_metadata::MetricType;
    #[allow(unused_imports)]
    pub use generated::read_request;
    pub use generated::read_request::ResponseType as ReadResponseType;
    #[allow(unused_imports)]
    pub use generated::{
        BucketSpan, Chunk, ChunkedReadResponse, ChunkedSeries, Exemplar, Histogram, Label,
        LabelMatcher, Labels, MetricMetadata, Query, QueryResult, ReadHints, ReadRequest,
        ReadResponse, Sample, TimeSeries, WriteRequest,
    };
}

/// OpenTelemetry metrics protobuf models.
pub mod otlp {
    /// Generated package hierarchy matching the upstream protobuf package names.
    #[allow(clippy::all, dead_code, missing_docs)]
    pub mod generated {
        pub mod opentelemetry {
            pub mod proto {
                pub mod collector {
                    pub mod metrics {
                        pub mod v1 {
                            include!(concat!(
                                env!("OUT_DIR"),
                                "/opentelemetry.proto.collector.metrics.v1.rs"
                            ));
                        }
                    }
                }
                pub mod common {
                    pub mod v1 {
                        include!(concat!(
                            env!("OUT_DIR"),
                            "/opentelemetry.proto.common.v1.rs"
                        ));
                    }
                }
                pub mod metrics {
                    pub mod v1 {
                        include!(concat!(
                            env!("OUT_DIR"),
                            "/opentelemetry.proto.metrics.v1.rs"
                        ));
                    }
                }
                pub mod resource {
                    pub mod v1 {
                        include!(concat!(
                            env!("OUT_DIR"),
                            "/opentelemetry.proto.resource.v1.rs"
                        ));
                    }
                }
            }
        }
    }

    pub use generated::opentelemetry::proto::collector::metrics::v1::{
        ExportMetricsServiceRequest, ExportMetricsServiceResponse,
    };
    pub use generated::opentelemetry::proto::common::v1::{
        any_value, AnyValue, ArrayValue, InstrumentationScope, KeyValue, KeyValueList,
    };
    pub use generated::opentelemetry::proto::metrics::v1::{
        exemplar, metric, number_data_point, AggregationTemporality, Exemplar, Gauge, Histogram,
        Metric, NumberDataPoint, Sum, Summary,
    };
    pub use generated::opentelemetry::proto::resource::v1::Resource;
}
