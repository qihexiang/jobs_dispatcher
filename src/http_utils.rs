use std::net::SocketAddr;

use axum::{
    extract::{ConnectInfo, State},
    http::{Request, StatusCode},
    middleware::Next,
    response::{Response, IntoResponse},
};

pub async fn client_host_check<B>(
    State(allow_list): State<Vec<String>>,
    ConnectInfo(connect): ConnectInfo<SocketAddr>,
    req: Request<B>,
    next: Next<B>,
) -> Response {
    let ip_addr = connect.ip();
    if allow_list.len() == 0 || allow_list.contains(&ip_addr.to_string()) {
        next.run(req).await
    } else if let Ok(hostname) = dns_lookup::lookup_addr(&ip_addr) {
        if allow_list.contains(&hostname) {
            next.run(req).await
        } else {
            (StatusCode::FORBIDDEN, "hostname not in allow list").into_response()
        }
    } else {
        (StatusCode::FORBIDDEN, "ip can't resovle and not in allow list").into_response()
    }
}
