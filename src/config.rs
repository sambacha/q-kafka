use std::env;

#[derive(Default, Debug, Clone)]
pub struct Config {
    pub log_level: String,
    pub broker: String,
    pub commands_topic: String,
    pub commands_group_id: String,
    pub events_topic: String,
    pub events_group_id: String,
}

pub trait Load {
    // Static method signature; `Self` refers to the implementor type
    fn load() -> Self;
}

impl Load for Config {
    fn load() -> Config {
        Config {
            log_level: get_env_var ("LOG_LEVEL", Some (String::from ("info"))),
            broker: get_env_var ("KAFKA_BROKER", Some (String::from ("localhost:9092"))),
            commands_topic: get_env_var ("KAFKA_COMMANDS_TOPICS", Some (String::from ("commands"))),
            commands_group_id: get_env_var ("KAFKA_COMMANDS_GROUP_ID", Some (String::from ("commands-processors"))),
            events_topic: get_env_var ("KAFKA_EVENTS_TOPICS", Some (String::from ("events"))),
            events_group_id: get_env_var ("KAFKA_EVENTS_GROUP_ID", Some (String::from ("events-processors"))),
        }
    }
}

fn get_env_var (var : &str, default: Option<String> ) -> String {
    match env::var(var) {
        Ok (v) => v,
        Err (_) => {
            match default {
                None => panic! ("Missing ENV variable: {} not defined in environment", var),
                Some (d) => d
            }
        }
    }
}
