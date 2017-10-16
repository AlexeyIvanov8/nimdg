
#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate serde_json;

use std::sync::atomic::AtomicUsize;
use std::io::BufWriter;
use std::fs::File;

use data_base::PersistenceError;
use data_base::transaction::Transaction;
use data_base::meta::TableDescription;

pub struct FileStorage {
    directory: String,
    snapshot_threshold: u32, // count of transactions, after that will begin shapshot creation
    transactions_count: AtomicUsize,
    file: BufWriter,
}

#[derive(Serialize, Deserialize, Debug)]
struct Meta {
    tables: Vec<TableDescription>,
}

impl FileStorage {
    fn open_or_create_file(path: String) -> Result<File, PersistenceError> {
        match File::open(path) {
            Ok(file) => file,
            Err(ioError) => File::create(path).map_err(|error| PersistenceError::FileStorageError(format!("error"))),
        }
    }

    pub fn new(directory: String, snapshot_threshold: u32) -> Result<FileStorage, PersistenceError> {
        let meta_file = open_or_create_file(format!("{}/meta.ndg", directory));
        let transactions_file_name = format!("{}/transactions.ndg", directory);
        let file = try!(File::open().map_err(|error| PersistenceError::FileStorageError(format!("{}", error))));
        FileStorage {
            transactions_count: AtomicUsize::new(0),
            snapshot_threshold: snapshot_threshold,
            directory: directory,
            file: BufWriter::new(File),
        }

    }

    fn create_snapshot() -> Result<(), PersistenceError> {
        Ok(())
    }

    fn save_transaction(transaction: &Transaction) -> Result<(), PersistenceError> {
        Ok(())
    }

    fn save_meta(data_base_manager: &DataBaseManager) -> Result<(), PersistenceError> {
        Ok(())
    }

    fn load_meta(directory: String, data_base_manage: &mut DataBaseManager) -> Result<(), PersistenceError> {
        let meta_file = try!(open_or_create_file(format!("{}/meta.ndg", directory)));
        let mut meta_description_str: String = String::new();
        let meta_file_reader = BufReader::new(meta_file);
        try!(meta_file_reader.read_to_string()
            .map_err(|error| PersistenceError::FileStorageError(format!("{}", error))));
        let meta_description: Meta = serde_json::from_str(&meta_description_str);
        debug!("Read meta {:?}", meta_description);
    }

    // fn load_data_base() -> Result<FileStorage, PersistenceError> {}
}
