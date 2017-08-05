
extern crate log4rs;

pub fn setup() {
    log4rs::init_file("config/log4rs.yml", Default::default()).unwrap();
}