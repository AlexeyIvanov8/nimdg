
extern crate serde;
extern crate serde_json;

use std::sync::atomic::AtomicUsize;
use std::io::{BufReader, BufWriter, Read, Write};
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
    table_names: Vec<String>,
}

macro_rules! convert_error {
    ($error:expr) => (PersistenceError::FileStorageError(format!("{}", $error)))
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

    fn save_table_description(&self, table_description: &TableDescription) -> Result<(), PersistenceError> {
        let mut desc_file = try!(FileStorage::open_or_create_file(format!("{}/meta/{}.tbl", self.directory, table_description.name)));
        let table_view = table_description.to_view();
        let json_table_view = try!(serde_json::to_string(&table_view).map_err(|error| PersistenceError::FileStorageError(format!("{}", error))));
        try!(write!(&desc_file, "{}", json_table_view).map_err(|error| PersistenceError::FileStorageError(format!("{}", error))));
        // try!(desc_file.write_fmt("{}", json_table_view)
        // .map_err(|error| PersistenceError::FileStorageError(format!("{}", error))));
        Ok(())
    }

    pub fn save_meta(&self, data_base_manager: &DataBaseManager) -> Result<(), PersistenceError> {
        let save_result: Result<Vec<String>, PersistenceError> = data_base_manager.table_descriptions
            .iter()
            .map(|(name, desc)| self.save_table_description(desc).map(|_| name.clone()))
            .collect();
        let table_names = try!(save_result);
        let mut common_meta_file = try!(FileStorage::open_or_create_file(format!("{}/meta/description.ndg", self.directory)));
        let meta = Meta { table_names: table_names };
        let json_meta = try!(serde_json::to_string(&meta).map_err(|error| PersistenceError::FileStorageError(format!("{}", error))));
        try!(write!(&common_meta_file, "{}", json_meta).map_err(|error| PersistenceError::FileStorageError(format!("{}", error))));
        Ok(())
    }

    fn load_table_description(&self, table_name: &String) -> Result<TableDescriptionView, PersistenceError> {
        let mut table_file = File::open(format!("{}/meta/{}.tbl", self.directory, table_name)).map_err(|error| convert_error!(error))?;
        let mut json_table_view = String::new();
        table_file.read_to_string(&mut json_table_view).map_err(|error| convert_error!(error))?;
        let table_desc: TableDescriptionView = serde_json::from_str(json_table_view.as_str()).map_err(|error| convert_error!(error))?;
        Ok(table_desc)
    }

    fn load_meta(&self, data_base_manager: &mut DataBaseManager) -> Result<(), PersistenceError> {
        let meta_file = try!(FileStorage::open_or_create_file(format!("{}/meta.ndg", self.directory)));
        let mut meta_description_str: String = String::new();
        let mut meta_file_reader = BufReader::new(meta_file);
        try!(meta_file_reader.read_to_string(&mut meta_description_str)
            .map_err(|error| PersistenceError::FileStorageError(format!("{}", error))));
        let meta_description: Meta = try!(serde_json::from_str(&meta_description_str)
            .map_err(|error| PersistenceError::IoEntity(IoEntityError::Read(format!("{}", error)))));
        debug!("Read meta {:?}", meta_description);

        let table_descs: Vec<TableDescriptionView> = try!(meta_description.table_names
            .iter()
            .map(|table_name| self.load_table_description(table_name))
            .collect());
        let res: Vec<String> = try!(table_descs.iter()
            .map(move |table_desc| data_base_manager.add_table(table_desc).map_err(|error| convert_error!(error)))
            .collect());
        Ok(())
    }

    // fn load_data_base() -> Result<FileStorage, PersistenceError> {}
}
