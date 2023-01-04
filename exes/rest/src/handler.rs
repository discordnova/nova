use std::{
    collections::hash_map::DefaultHasher,
    convert::TryFrom,
    hash::{Hash, Hasher},
    str::FromStr,
    time::Instant,
};

use anyhow::bail;
use http::{
    header::{AUTHORIZATION, CONNECTION, HOST, TRANSFER_ENCODING, UPGRADE},
    HeaderValue, Method as HttpMethod, Request, Response, Uri,
};
use hyper::{client::HttpConnector, Body, Client};
use hyper_tls::HttpsConnector;
use shared::log::error;
use twilight_http_ratelimiting::{Method, Path};

use crate::ratelimit_client::RemoteRatelimiter;

/// Normalizes the path
fn normalize_path(request_path: &str) -> (&str, &str) {
    if let Some(trimmed_path) = request_path.strip_prefix("/api") {
        if let Some(maybe_api_version) = trimmed_path.split('/').nth(1) {
            if let Some(version_number) = maybe_api_version.strip_prefix('v') {
                if version_number.parse::<u8>().is_ok() {
                    let len = "/api/v".len() + version_number.len();
                    return (&request_path[..len], &request_path[len..]);
                };
            };
        }

        ("/api", trimmed_path)
    } else {
        ("/api", request_path)
    }
}

pub async fn handle_request(
    client: Client<HttpsConnector<HttpConnector>, Body>,
    ratelimiter: RemoteRatelimiter,
    token: &str,
    mut request: Request<Body>,
) -> Result<Response<Body>, anyhow::Error> {
    let (hash, uri_string) = {
        let method = match *request.method() {
            HttpMethod::DELETE => Method::Delete,
            HttpMethod::GET => Method::Get,
            HttpMethod::PATCH => Method::Patch,
            HttpMethod::POST => Method::Post,
            HttpMethod::PUT => Method::Put,
            _ => {
                error!("Unsupported HTTP method in request, {}", request.method());
                bail!("unsupported method");
            }
        };

        let request_path = request.uri().path();
        let (api_path, trimmed_path) = normalize_path(request_path);

        let mut uri_string = format!("https://discord.com{}{}", api_path, trimmed_path);
        if let Some(query) = request.uri().query() {
            uri_string.push('?');
            uri_string.push_str(query);
        }

        let mut hash = DefaultHasher::new();
        match Path::try_from((method, trimmed_path)) {
            Ok(path) => path,
            Err(e) => {
                error!(
                    "Failed to parse path for {:?} {}: {:?}",
                    method, trimmed_path, e
                );
                bail!("failed o parse");
            }
        }
        .hash(&mut hash);

        (hash.finish().to_string(), uri_string)
    };

    let start_ticket_request = Instant::now();
    let header_sender = match ratelimiter.ticket(hash).await {
        Ok(sender) => sender,
        Err(e) => {
            error!("Failed to receive ticket for ratelimiting: {:?}", e);
            bail!("failed to reteive ticket");
        }
    };
    let time_took_ticket = Instant::now() - start_ticket_request;

    request.headers_mut().insert(
        AUTHORIZATION,
        HeaderValue::from_bytes(token.as_bytes())
            .expect("strings are guaranteed to be valid utf-8"),
    );
    request
        .headers_mut()
        .insert(HOST, HeaderValue::from_static("discord.com"));

    // Remove forbidden HTTP/2 headers
    // https://datatracker.ietf.org/doc/html/rfc7540#section-8.1.2.2
    request.headers_mut().remove(CONNECTION);
    request.headers_mut().remove("keep-alive");
    request.headers_mut().remove("proxy-connection");
    request.headers_mut().remove(TRANSFER_ENCODING);
    request.headers_mut().remove(UPGRADE);
    request.headers_mut().remove(AUTHORIZATION);
    request.headers_mut().append(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bot {}", token))?,
    );

    let uri = match Uri::from_str(&uri_string) {
        Ok(uri) => uri,
        Err(e) => {
            error!("Failed to create URI for requesting Discord API: {:?}", e);
            bail!("failed to create uri");
        }
    };
    *request.uri_mut() = uri;

    let start_upstream_req = Instant::now();
    let mut resp = match client.request(request).await {
        Ok(response) => response,
        Err(e) => {
            error!("Error when requesting the Discord API: {:?}", e);
            bail!("failed to request the discord api");
        }
    };
    let upstream_time_took = Instant::now() - start_upstream_req;

    resp.headers_mut().append(
        "X-TicketRequest-Ms",
        HeaderValue::from_str(&time_took_ticket.as_millis().to_string()).unwrap(),
    );
    resp.headers_mut().append(
        "X-Upstream-Ms",
        HeaderValue::from_str(&upstream_time_took.as_millis().to_string()).unwrap(),
    );
    
    let ratelimit_headers = resp
        .headers()
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_str().unwrap().to_string()))
        .collect();

    if header_sender.send(ratelimit_headers).is_err() {
        error!("Error when sending ratelimit headers to ratelimiter");
    };

    Ok(resp)
}
