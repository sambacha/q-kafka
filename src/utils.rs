pub mod utils {

    pub fn print_type_of<T>(_: &T) {
        println!("{}", std::any::type_name::<T>())
    }

    pub fn string_to_vector (s : &str) -> Vec<String> {
        s.split(",").map(|x| String::from (x)).collect()
    }
}
