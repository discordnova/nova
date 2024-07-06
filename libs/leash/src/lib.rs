#![deny(
    clippy::all,
    clippy::correctness,
    clippy::suspicious,
    clippy::style,
    clippy::complexity,
    clippy::perf,
    clippy::pedantic,
    clippy::nursery
)]

use anyhow::Result;
use opentelemetry::global::shutdown_tracer_provider;
use opentelemetry::{global, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{trace, Resource};
use serde::de::DeserializeOwned;
use shared::config::Settings;
use std::str::FromStr;
use std::{future::Future, pin::Pin};
use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tracing::{error, info, trace};
use tracing_subscriber::filter::Directive;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

pub type AnyhowResultFuture<T> = Pin<Box<dyn Future<Output = Result<T>> + Send>>;
pub trait Component: Send + Sync + 'static + Sized {
    type Config: Default + Clone + DeserializeOwned + Send;

    const SERVICE_NAME: &'static str;
    fn start(
        &self,
        settings: Settings<Self::Config>,
        stop: oneshot::Receiver<()>,
    ) -> AnyhowResultFuture<()>;
    fn new() -> Self;
}

/// # Panics
/// Panics in case of an invalid `RUST_LOG` variable.
pub fn start_component<Y, C>(component: Y) -> AnyhowResultFuture<()>
where
    Y: Component<Config = C>,
    C: Default + Clone + DeserializeOwned + Send,
{
    Box::pin(async move {
        let settings = Settings::<Y::Config>::new(Y::SERVICE_NAME)?;

        if let Some(meter_config) = settings
            .opentelemetry
            .as_ref()
            .and_then(|f| f.metrics.clone())
        {
            let otlp_exporter = opentelemetry_otlp::new_exporter().tonic();
            // Then pass it into pipeline builder
            let _ = opentelemetry_otlp::new_pipeline()
                .tracing()
                .with_exporter(otlp_exporter)
                .install_simple()?;
        }
        // Print debug errors
        global::set_error_handler(|error| {
            error!("OpenTelemetry error: {}", error);
        })?;

        if let Some(tracer_config) = settings
            .opentelemetry
            .as_ref()
            .and_then(|f| f.traces.clone())
        {
            let tracer = opentelemetry_otlp::new_pipeline()
                .tracing()
                .with_trace_config(trace::config().with_resource(Resource::new(vec![
                    KeyValue::new("service.name", Y::SERVICE_NAME),
                ])))
                .with_exporter(
                    opentelemetry_otlp::new_exporter()
                        .tonic()
                        .with_export_config(tracer_config.into()),
                )
                .install_batch(opentelemetry_sdk::runtime::Tokio)?;
            let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

            tracing_subscriber::registry()
                .with(fmt::layer())
                .with(otel_layer)
                .with(
                    // Use the info level as default
                    EnvFilter::builder()
                        .with_default_directive(Directive::from_str("info").unwrap())
                        .from_env()?,
                )
                .init();
        } else {
            // Setup tracing
            tracing_subscriber::registry()
                .with(fmt::layer())
                .with(
                    // Use the info level as default
                    EnvFilter::builder()
                        .with_default_directive(Directive::from_str("info").unwrap())
                        .from_env()?,
                )
                .init();
        }

        // Finally starting nova
        info!("Starting nova component {}", Y::SERVICE_NAME);
        let (stop, stop_channel) = oneshot::channel();

        tokio::spawn(async move {
            trace!("started signal watching");
            #[cfg(unix)]
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .unwrap()
                .recv()
                .await;
            #[cfg(not(unix))]
            tokio::signal::ctrl_c().await.unwrap();

            stop.send(()).unwrap();
            shutdown_tracer_provider();
        });

        trace!(
            "Starting component {component}",
            component = Y::SERVICE_NAME
        );
        component.start(settings, stop_channel).await
    })
}

#[macro_export]
macro_rules! ignite {
    ($c:ty) => {
        #[allow(dead_code)]
        fn main() -> anyhow::Result<()> {
            use $crate::Component;
            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on($crate::start_component(<$c as Component>::new()))?;
            Ok(())
        }
    };
}

#[cfg(test)]
mod test {
    use serde::Deserialize;
    use tokio::sync::oneshot;

    use crate as leash;

    #[derive(Clone, Copy)]
    struct TestComponent {}

    #[derive(Default, Clone, Deserialize, Copy)]
    struct TestComponentConfig {}

    impl leash::Component for TestComponent {
        type Config = TestComponentConfig;
        const SERVICE_NAME: &'static str = "test_component";

        fn start(
            &self,
            _settings: shared::config::Settings<Self::Config>,
            _stop: oneshot::Receiver<()>,
        ) -> crate::AnyhowResultFuture<()> {
            Box::pin(async move { Ok(()) })
        }

        fn new() -> Self {
            Self {}
        }
    }

    ignite!(TestComponent);
}
