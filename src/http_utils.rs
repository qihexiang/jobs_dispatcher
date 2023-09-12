use std::collections::HashMap;

use axum::{
    TypedHeader,
    headers::{Authorization, authorization::Basic},
    extract::State,
    http::{Request, StatusCode},
    middleware::Next,
    response::{Response, IntoResponse},
};

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
