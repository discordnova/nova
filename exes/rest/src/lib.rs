#![deny(
    clippy::all,
    clippy::correctness,
    clippy::suspicious,
    clippy::style,
    clippy::complexity,
    clippy::perf,
    clippy::pedantic,
    clippy::nursery,
    unsafe_code
)]

use config::ReverseProxy;

use handler::handle_request;
use hyper::{
    server::conn::AddrStream,
    service::{make_service_fn, service_fn},
    Body, Client, Request, Server,
};
use leash::{AnyhowResultFuture, Component};
use opentelemetry::global;
use opentelemetry_http::HeaderExtractor;
use shared::config::Settings;
use std::{convert::Infallible, sync::Arc};
use tokio::sync::oneshot;
use tracing_opentelemetry::OpenTelemetrySpanExt;

mod config;
mod handler;
mod ratelimit_client;

pub struct ReverseProxyServer {}
impl Component for ReverseProxyServer {
    type Config = ReverseProxy;
    const SERVICE_NAME: &'static str = "rest";

    fn start(
        &self,
        settings: Settings<Self::Config>,
        stop: oneshot::Receiver<()>,
    ) -> AnyhowResultFuture<()> {
        Box::pin(async move {
            // Client to the remote ratelimiters
            let ratelimiter = Arc::new(ratelimit_client::RemoteRatelimiter::new(
                settings.config.clone(),
            ));
            let https = hyper_rustls::HttpsConnectorBuilder::new()
                .with_native_roots()
                .expect("Native roots not available")
                .https_or_http()
                .enable_http1()
                .build();

            let client: Client<_, hyper::body::Body> = Client::builder().build(https);
            let token = settings.config.discord.token.clone();
            let config = settings.config.clone();
            let service_fn = make_service_fn(move |_: &AddrStream| {
                let client = client.clone();
                let ratelimiter = ratelimiter.clone();
                let token = token.clone();
                let config = config.clone();
                async move {
                    Ok::<_, Infallible>(service_fn(move |request: Request<Body>| {
                        let token = token.clone();
                        let parent_cx = global::get_text_map_propagator(|propagator| {
                            propagator.extract(&HeaderExtractor(request.headers()))
                        });

                        let span = tracing::span!(tracing::Level::INFO, "request process");
                        span.set_parent(parent_cx);

                        let client = client.clone();
                        let ratelimiter = ratelimiter.clone();

                        let config = config.clone();
                        async move {
                            let token = token.clone();
                            let ratelimiter = ratelimiter.clone();
                            handle_request(client, ratelimiter, config, token, request).await
                        }
                    }))
                }
            });

            let server = Server::bind(&settings.config.server.listening_adress)
                .http1_only(true)
                .serve(service_fn);

            server
                .with_graceful_shutdown(async {
                    stop.await.expect("should not fail");
                })
                .await?;

            Ok(())
        })
    }

    fn new() -> Self {
        Self {}
    }
}
