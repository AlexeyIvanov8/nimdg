
extern crate bincode;
extern crate serde;
extern crate serde_json;

use std::sync::atomic::AtomicUsize;
use std::io::{BufReader, BufWriter, Read, Write};
use std::fs::{File, OpenOptions};
use std::fs;
use std::sync::Mutex;

use data_base::{DataBaseManager, Entity, Field};
use data_base::{PersistenceError, IoEntityError};
use data_base::transaction::Transaction;
use data_base::meta::{TableDescription, TableDescriptionView};
use std::collections::{BTreeSet, BTreeMap};

use self::serde::ser::Serialize;
use self::serde::de::Deserialize;
use bincode::{serialize, deserialize, Infinite};

pub struct FileStorageBase {
    directory: String,
    snapshot_threshold: u32, // count of transactions, after that will begin shapshot creation
    transactions_count: AtomicUsize,
    file: Mutex<BufWriter<File>>,
}

pub struct FileStorageEmpty {}

#[derive(Serialize, Deserialize, Debug)]
struct Meta {
    table_names: BTreeSet<String>,
}

#[derive(Serialize, Deserialize)]
enum OperationType {
    Insert,
    Delet,
}

// Because lock is not serializable
#[derive(Serialize, Deserialize)]
struct PersistEntity {
    fields: BTreeMap<u16, Field>,
}

#[derive(serde::ser::Serialize, self::serde::de::Deserialize)]
struct TransactionalOperation {
    table_name: String,
    key: PersistEntity,
    value: PersistEntity,
    operation_type: OperationType,
}

macro_rules! convert_error {
    ($error:expr) => (PersistenceError::FileStorageError(format!("{}", $error)))
}

pub trait FileStorage {
    fn create_snapshot(&self) -> Result<(), PersistenceError>;
    fn save_transaction(&self, transaction: &Transaction) -> Result<(), PersistenceError>;
    fn update_table_description(&self, table_description: &TableDescription) -> Result<(), PersistenceError>;
    fn save_data_base(&self, data_base_manager: &DataBaseManager) -> Result<(), PersistenceError>;
    fn load_data_base(&self, data_base_manager: &mut DataBaseManager) -> Result<(), PersistenceError>;
}

impl FileStorageBase {
    fn open_or_create_file(path: String) -> Result<File, PersistenceError> {
        match OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            //.append(true)
            //.truncate(true)
            .open(path.clone())
            /*File::open(path.clone())*/ {
            Ok(file) => {
                debug!("File {} succefull open", path);
                Ok(file)
            },
            Err(_) => {
                debug!("File {} dont open and will be created", path);
                File::create(path.clone()).map_err(|error| PersistenceError::FileStorageError(format!("open {} fail: {}", path, error)))
            },
        }
    }

    pub fn new(directory: String, snapshot_threshold: u32) -> Result<FileStorageBase, PersistenceError> {
        fs::create_dir_all(directory.clone()).map_err(|error| convert_error!(error))?;
        // let meta_file = FileStorageBase::open_or_create_file(format!("{}/meta.ndg", directory))?;
        let transactions_file_name = format!("{}/transactions.ndg", directory);
        let file = FileStorageBase::open_or_create_file(transactions_file_name)?;
        Ok(FileStorageBase {
            transactions_count: AtomicUsize::new(0),
            snapshot_threshold: snapshot_threshold,
            directory: directory,
            file: Mutex::new(BufWriter::new(file)),
        })
    }

    fn save_table_description(&self, table_description: &TableDescription) -> Result<(), PersistenceError> {
        debug!("Begin save {} table description", table_description.name);

        let path = format!("{}/meta/{}.tbl", self.directory, table_description.name);
        fs::create_dir_all(format!("{}/meta", self.directory))
            .map_err(|error| PersistenceError::FileStorageError(format!("Create dir {}/meta error: {}", self.directory, error)))?;
        let desc_file = try!(FileStorageBase::open_or_create_file(path.clone()));
        try!(desc_file.set_len(0).map_err(|error| PersistenceError::FileStorageError(format!("Fail set len = 0 for {}: {}", path, error))));
        let table_view = table_description.to_view();
        let json_table_view = try!(serde_json::to_string(&table_view).map_err(|error| PersistenceError::FileStorageError(format!("{}", error))));
        try!(write!(&desc_file, "{}", json_table_view)
            .map_err(|error| PersistenceError::FileStorageError(format!("Write {} error: {}", path.clone(), error))));
        debug!("{} table description saved to {}",
               table_description.name,
               path);
        Ok(())
    }

    fn save_meta(&self, meta: &Meta) -> Result<(), PersistenceError> {
        let common_meta_file = try!(FileStorageBase::open_or_create_file(format!("{}/meta/description.ndg", self.directory)));
        let json_meta = try!(serde_json::to_string(meta).map_err(|error| PersistenceError::FileStorageError(format!("{}", error))));
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

    fn load_meta(&self) -> Result<Meta, PersistenceError> {
        let path = format!("{}/meta/description.ndg", self.directory);
        let mut meta_file = try!(FileStorageBase::open_or_create_file(path.clone()));
        let mut meta_description_str: String = String::new();
        let metadata = meta_file.metadata().unwrap();
        // let mut meta_file_reader = BufReader::new(meta_file);
        try!(// meta_file_reader.read_to_string(&mut meta_description_str)
             meta_file.read_to_string(&mut meta_description_str)
            .map_err(|error| {
                PersistenceError::FileStorageError(format!("Read meta from {}, metadata = {:?}, error: {}",
                                                           path,
                                                           metadata,
                                                           error))
            }));
        debug!("Load meta_description_str = {}", meta_description_str);
        if meta_description_str.is_empty() {
            Ok(Meta { table_names: BTreeSet::new() })
        } else {
            let meta: Meta = try!(serde_json::from_str(&meta_description_str).map_err(|error| {
                PersistenceError::IoEntity(IoEntityError::Read(format!("Read meta from JS {} error: {}",
                                                                       meta_description_str,
                                                                       error)))
            }));
            debug!("Read meta {:?}", meta);
            Ok(meta)
        }
    }
}

impl PersistEntity {
    pub fn new(entity: &Entity) -> PersistEntity {
        PersistEntity { fields: entity.fields.clone() }
    }
}

impl FileStorage for FileStorageBase {
    fn create_snapshot(&self) -> Result<(), PersistenceError> {
        Ok(())
    }

    fn save_transaction(&self, transaction: &Transaction) -> Result<(), PersistenceError> {
        let mut writer = self.file.lock().unwrap();
        transaction.get_locked_keys()
            .iter()
            .for_each(|(key, value)| {
                let transactional_operation = TransactionalOperation {
                    table_name: key.table_name.clone(),
                    key: PersistEntity::new(&key.key),
                    value: PersistEntity::new(&value.value),
                    operation_type: OperationType::Insert,
                };
                let encoded: Vec<u8> = serialize(&transactional_operation, Infinite).unwrap();
                writer.write(&encoded);
            });
        Ok(())
    }

    fn update_table_description(&self, table_description: &TableDescription) -> Result<(), PersistenceError> {
        try!(self.save_table_description(table_description));
        let mut meta: Meta = try!(self.load_meta());
        meta.table_names.insert(table_description.name.clone());
        try!(self.save_meta(&meta));
        debug!("meta {:?} is saved", meta);
        Ok(())
    }

    fn save_data_base(&self, data_base_manager: &DataBaseManager) -> Result<(), PersistenceError> {
        let save_result: Result<BTreeSet<String>, PersistenceError> = data_base_manager.tables
            .iter()
            .map(|(name, table)| self.save_table_description(&(table.description)).map(|_| name.clone()))
            .collect();
        let table_names = try!(save_result);
        let meta = Meta { table_names: table_names };
        try!(self.save_meta(&meta));
        // let json_meta = try!(serde_json::to_string(&meta).map_err(|error| PersistenceError::FileStorageError(format!("{}", error))));
        // try!(write!(&common_meta_file, "{}", json_meta).map_err(|error| PersistenceError::FileStorageError(format!("{}", error))));
        Ok(())
    }

    fn load_data_base(&self, data_base_manager: &mut DataBaseManager) -> Result<(), PersistenceError> {
        let meta: Meta = try!(self.load_meta());
        debug!("Loaded meta = {:?}", meta);
        let table_descs: Vec<TableDescriptionView> = try!(meta.table_names
            .iter()
            .map(|table_name| self.load_table_description(table_name))
            .collect());
        debug!("Loaded tables descs = {:?}", table_descs);
        let res: Vec<String> = try!(table_descs.iter()
            .map(move |table_desc| data_base_manager.add_table(table_desc).map_err(|error| convert_error!(error)))
            .collect());
        Ok(())
    }

    // fn load_data_base() -> Result<FileStorage, PersistenceError> {}
}

impl FileStorage for FileStorageEmpty {
    fn create_snapshot(&self) -> Result<(), PersistenceError> {
        Ok(())
    }
    fn save_transaction(&self, transaction: &Transaction) -> Result<(), PersistenceError> {
        Ok(())
    }
    fn update_table_description(&self, table_description: &TableDescription) -> Result<(), PersistenceError> {
        Ok(())
    }
    fn save_data_base(&self, data_base_manager: &DataBaseManager) -> Result<(), PersistenceError> {
        Ok(())
    }
    fn load_data_base(&self, data_base_manager: &mut DataBaseManager) -> Result<(), PersistenceError> {
        Ok(())
    }
}
