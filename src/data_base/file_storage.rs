
extern crate serde;
extern crate serde_json;

use std::sync::atomic::AtomicUsize;
use std::io::{BufReader, BufWriter, Read};
use std::fs::File;

use data_base::DataBaseManager;
use data_base::{PersistenceError, IoEntityError};
use data_base::transaction::Transaction;
use data_base::meta::{TableDescription, TableDescriptionView};

pub struct FileStorage {
    directory: String,
    snapshot_threshold: u32, // count of transactions, after that will begin shapshot creation
    transactions_count: AtomicUsize,
    file: BufWriter<File>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Meta {
    tables: Vec<TableDescriptionView>,
}

impl FileStorage {
    fn open_or_create_file(path: String) -> Result<File, PersistenceError> {
        match File::open(path.clone()) {
            Ok(file) => Ok(file),
            Err(_) => File::create(path.clone()).map_err(|error| PersistenceError::FileStorageError(format!("{}", error))),
        }
    }

    pub fn new(directory: String, snapshot_threshold: u32) -> Result<FileStorage, PersistenceError> {
        let meta_file = FileStorage::open_or_create_file(format!("{}/meta.ndg", directory));
        let transactions_file_name = format!("{}/transactions.ndg", directory);
        let file = try!(File::open(transactions_file_name).map_err(|error| PersistenceError::FileStorageError(format!("{}", error))));
        Ok(FileStorage {
            transactions_count: AtomicUsize::new(0),
            snapshot_threshold: snapshot_threshold,
            directory: directory,
            file: BufWriter::new(file),
        })
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
        let meta_file = try!(FileStorage::open_or_create_file(format!("{}/meta.ndg", directory)));
        let mut meta_description_str: String = String::new();
        let mut meta_file_reader = BufReader::new(meta_file);
        try!(meta_file_reader.read_to_string(&mut meta_description_str)
            .map_err(|error| PersistenceError::FileStorageError(format!("{}", error))));
        let meta_description: Meta = try!(serde_json::from_str(&meta_description_str)
            .map_err(|error| PersistenceError::IoEntity(IoEntityError::Read(format!("{}", error)))));
        debug!("Read meta {:?}", meta_description);
        Ok(())
    }

    // fn load_data_base() -> Result<FileStorage, PersistenceError> {}
}
