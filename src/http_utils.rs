use std::net::SocketAddr;

use axum::{
    extract::{ConnectInfo, State},
    http::{Request, StatusCode},
    middleware::Next,
    response::Response,
};

pub async fn client_host_check<B>(
    State(allow_list): State<Vec<String>>,
    ConnectInfo(connect): ConnectInfo<SocketAddr>,
    req: Request<B>,
    next: Next<B>,
) -> Result<Response, StatusCode> {
    let ip_addr = connect.ip();
    if allow_list.len() == 0 || allow_list.contains(&ip_addr.to_string()) {
        Ok(next.run(req).await)
    } else if let Ok(hostname) = dns_lookup::lookup_addr(&ip_addr) {
        if allow_list.contains(&hostname) {
            Ok(next.run(req).await)
        } else {
            Err(StatusCode::FORBIDDEN)
        }
    } else {
        Err(StatusCode::FORBIDDEN)
    }
}
