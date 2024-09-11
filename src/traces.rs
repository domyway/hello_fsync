use std::fs::remove_file;
use std::net::SocketAddr;
use opentelemetry::{global, Key};
use opentelemetry::trace::{Span, TraceError, Tracer};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_proto::tonic::collector::trace::v1::{
    trace_service_server::TraceService, ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::TraceServiceServer;
use tokio::sync::Mutex;
use tonic::{codegen::*, Response};
use crate::{fsync_benchmark, Writer};
use opentelemetry_sdk::{propagation::TraceContextPropagator, trace as sdktrace};
use opentelemetry_sdk::trace::BatchConfigBuilder;

#[derive(Default)]
pub struct TraceServer {}

#[async_trait]
impl TraceService for TraceServer {
    async fn export(
        &self,
        _: tonic::Request<ExportTraceServiceRequest>,
    ) -> Result<tonic::Response<ExportTraceServiceResponse>, tonic::Status> {
        let file_path = "/data/test_fsync_benchmark";
        // delete the file if it already exists
        if std::path::Path::new(&file_path).exists() {
            remove_file(&file_path).expect("Failed to delete file");
        }
        let init_size = 1024 * 1024 * 128; // 128 MB
        let buffer_size = 1024 * 16; // 16 KB
        let writer = Writer::new(file_path, init_size, buffer_size).expect("Failed to create writer");


        let file_lock = Arc::new(Mutex::new(writer));
        fsync_benchmark(file_lock, 4 * 1024, 1).await.expect("benchmark failed");

        Ok(Response::new(ExportTraceServiceResponse {
            partial_success: None,
        }))
    }
}


pub async fn init_common_grpc_server() -> Result<(), anyhow::Error> {
    let ip = "0.0.0.0".to_string();
    let gaddr: SocketAddr = format!("{}:{}", ip, 5081).parse()?;

    let tracer = TraceServer::default();
    let trace_svc = TraceServiceServer::new(tracer)
        .send_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Gzip);

    tonic::transport::Server::builder()
        .add_service(trace_svc)
        .serve(gaddr)
        .await
        .expect("gRPC server init failed");
    Ok(())
}


pub fn init_tracer_otlp() -> Result<sdktrace::Tracer, TraceError> {
    // Start a new jaeger trace pipeline
    global::set_text_map_propagator(TraceContextPropagator::new());
    let exporter = opentelemetry_otlp::new_exporter()
        .tonic()
        .with_endpoint("http://localhost:5081");
    let batch_config = BatchConfigBuilder::default()
        .with_max_queue_size(20480)   // 设置更大的缓冲区
        .with_scheduled_delay(std::time::Duration::from_millis(1))
        .build();

    opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(exporter)
        .with_batch_config(batch_config)
        .install_simple()
}


pub async fn create_span_with_trace_id(trace_id: &str) {
    let tracer = global::tracer("example-tracer");

    let mut span = tracer
        .span_builder("example-operation")
        .with_kind(opentelemetry::trace::SpanKind::Client)
        .with_trace_id(opentelemetry::trace::TraceId::from_hex(trace_id).unwrap()) // Parse trace ID
        .start(&tracer);
    // Add attributes, events, or other span operations
    span.add_event(
        "Nice operation!".to_string(),
        vec![Key::new("bogons").i64(100)],
    );

    // Optionally, end the span
    span.end();
    // println!("{:?}", span.span_context());
}