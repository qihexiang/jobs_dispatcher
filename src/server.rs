use serde::{Deserialize, Serialize};
use std::{net::IpAddr, collections::HashMap};

use axum::{
    TypedHeader,
    headers::{Authorization, authorization::Basic},
    extract::State,
    http::{Request, StatusCode},
    middleware::Next,
    response::{Response, IntoResponse},
};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct HttpServerConfig {
    pub ip: IpAddr,
    pub port: u16,
}

impl Default for HttpServerConfig {
    fn default() -> Self {
        HttpServerConfig {
            ip: IpAddr::from([0, 0, 0, 0]),
            port: 9500,
        }
    }
}

pub async fn basic_check<B>(
    State(user_table): State<HashMap<String, String>>,
    TypedHeader(Authorization(basic)): TypedHeader<Authorization<Basic>>,
    req: Request<B>, next: Next<B>
) -> Response {
    let username = basic.username();
    let password = basic.password();
    if user_table.get(username).map(|pw| pw == password).unwrap_or(false) {
        next.run(req).await
    } else {
        (StatusCode::FORBIDDEN, "Require auth").into_response()
    }
}
