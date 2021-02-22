use crate::commands;
use crate::queries;
use crate::config::{Config};
use crate::db::Db;
use crate::producer::Producer;
use crate::producer;
use std::convert::Infallible;
use std::sync::Arc;
use uuid::Uuid;
use warp::Filter;
use crate::admin;
use crate::admin::KafkaAdmin;

/// writes to commands topic
/// enforces light schema validation
pub async fn run (config: Arc<Config>, db: Db, admin: KafkaAdmin) {

    let config = &*config;
    let producer = producer::init (&config);
    let routes = create_value(producer.clone (), config.clone ())
        .or(update_value(producer.clone (), config.clone ()))
        .or (query_value (db));

    // create commands topic
     admin::create_topic (admin, &config.commands_topic).await;

    warp::serve(routes)
        .run(([127, 0, 0, 1], 3030))
        .await;
}

/// POST /values {"value" : 2 }
fn create_value(
    producer : Producer,
    config: Config
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path("values")
        .and(warp::post())
        .and(warp::body::json())
        .and(with_producer(producer))
        .and(with_config(config))
        .and_then(commands::create_value)
}

/// PUT /values/:id {"operation" : "add", "value" : 2 }
fn update_value(
    producer : Producer,
    config: Config
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("values" / Uuid)
        .and(warp::put())
        .and(warp::body::json())
        .and(with_producer(producer))
        .and(with_config(config))
        .and_then(commands::update_value)
}

/// GET /values/:id { "value" : 2 }
fn query_value(
    db : Db
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("values" / Uuid)
        .and(warp::get())
        .and(with_db(db))
        .and_then(queries::get_value)
}

fn with_producer(producer: Producer) -> impl Filter<Extract = (Producer,), Error = Infallible> + Clone {
    warp::any().map(move || producer.clone())
}

fn with_config(config: Config) -> impl Filter<Extract = (Config,), Error = Infallible> + Clone {
    warp::any().map(move || config.clone())
}

fn with_db(db: Db) -> impl Filter<Extract = (Db,), Error = Infallible> + Clone {
    warp::any().map(move || db.clone())
}
