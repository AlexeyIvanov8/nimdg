
pub struct AppDataBase;
impl iron::typemap::Key for AppDataBase {
    type Value = DataBaseManager;
}

pub trait DataBaseExtension: rustless::Extensible {
    fn get_data_base_manager(&self) -> &DataBaseManager;
}

impl DataBaseExtension for rustless::Application {
    fn get_data_base_manager(&self) -> &DataBaseManager {
        self.ext().get::<AppDataBase>().unwrap()
    }
}