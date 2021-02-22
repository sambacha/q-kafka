use crate::commands_schema::{Value};
use crate::db::Db;
use crate::db;
use log::info;
use std::convert::Infallible;
use uuid::Uuid;

pub async fn get_value(
    value_id: Uuid,
    db: Db
) -> Result<impl warp::Reply, Infallible> {

    info!("Querying value id {} ", value_id);

    match db::get (&db, &value_id).await {
        None => Ok(warp::reply::with_status(warp::reply::json (&format!("No value with id {} exists", &value_id)),
                                            warp::http::StatusCode::NO_CONTENT)),
        Some (value) => {
            let body = Value {value_id : value_id,
                              value : value};
            Ok(warp::reply::with_status(warp::reply::json(&body),
                                        warp::http::StatusCode::ACCEPTED))
        }

    }

}
