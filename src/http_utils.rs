use std::{net::SocketAddr, collections::HashMap};

use base64::{engine::general_purpose::URL_SAFE, Engine};
use regex::Regex;

use lazy_static::lazy_static;

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

pub async fn basic_check<B>(
    State(user_table): State<HashMap<String, String>>,
    req: Request<B>, next: Next<B>
) -> Response {
    lazy_static! {
        static ref BASIC_AUTH_RE: Regex = Regex::new("^Basic (?P<token>[A-Za-z0-9+/]+={0,2})$").unwrap();
    }
    if user_table.len() == 0 {
        next.run(req).await
    } else if let Some(Ok(authorization)) = req.headers().get("Authorization").map(|header| header.to_str()) {
        if let Some(Some(token)) = BASIC_AUTH_RE.captures(authorization).map(|cap| cap.name("token").map(|m| m.as_str())) {
            let mut parsed = String::new();
            URL_SAFE.encode_string(token, &mut parsed);
            if let Some((username, password)) = parsed.split_once(":") {
                if user_table.get(username).map(|pw| pw == password).unwrap_or(false) {
                    next.run(req).await
                } else {
                    (StatusCode::FORBIDDEN, "Invalid username or password").into_response()
                }
            } else {
                (StatusCode::BAD_REQUEST, "Invalid Authorization header").into_response()
            }
        } else {
            (StatusCode::BAD_REQUEST, "Invalid Authorization header").into_response()
        }
    } else {
        (StatusCode::FORBIDDEN, "No Authorization header found").into_response()
    }
}