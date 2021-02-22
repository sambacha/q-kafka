use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

pub type Db = Arc<Mutex<HashMap<Uuid, f64>>>;

/// atomic, thread safe in-memory db
pub fn init () -> Db {
    Arc::new(Mutex::new(HashMap::new()))
}

pub async fn insert (db: &Db, key : Uuid , value : f64) {
    let mut db = db.lock().await;
    db.insert (key, value);
}

pub async fn get (db: &Db, key : &Uuid) -> Option<f64> {
    let db = db.lock().await;
    match db.get (key) {
        None => None,
        Some (v) => Some (*v)
    }
}
