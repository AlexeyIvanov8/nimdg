
extern crate serde;
extern crate serde_json;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::boxed::Box;
use std::sync::atomic::{AtomicUsize, Ordering};

use rustless::json::ToJson;
use rustless;

use data_base::IoEntityError;

// Type trait, that allow define user type
#[derive(Serialize, Deserialize)]
pub struct TypeDescription {
    pub name: String,
    pub reader: Box<Fn(&rustless::json::JsonValue) -> Result<Vec<u8>, IoEntityError>>,
    pub writer: Box<Fn(&Vec<u8>) -> Result<rustless::json::JsonValue, IoEntityError>>,
}

// Universal description of some entity. For example: key or value
// For performance purposes each field is marked by number id
#[derive(Serialize, Deserialize)]
pub struct EntityDescription {
    count: AtomicUsize,
    pub fields: BTreeMap<String, Arc<Box<TypeDescription>>>,
    pub ids_map: BTreeMap<u16, String>,
    pub reverse_ids_map: BTreeMap<String, u16>,
}

// Description of table, that is key-value cache
#[derive(Serialize, Deserialize)]
pub struct TableDescription {
    pub name: String,
    pub key: EntityDescription,
    pub value: EntityDescription,
}

// For getting from frontend
#[derive(Debug)]
pub struct EntityDescriptionView {
    pub fields: BTreeMap<String, String>,
}

#[derive(Debug)]
pub struct TableDescriptionView {
    pub name: String,
    pub key: EntityDescriptionView,
    pub value: EntityDescriptionView,
}

// TypeDescription impl
unsafe impl Send for TypeDescription {}
unsafe impl Sync for TypeDescription {}

impl ToJson for TypeDescription {
    fn to_json(&self) -> rustless::json::JsonValue {
        rustless::json::to_value(self.name.clone())
    }
}

impl EntityDescriptionView {
    fn from_json(json: &BTreeMap<String, rustless::json::JsonValue>) -> Result<EntityDescriptionView, IoEntityError> {
        match json.get("fields") {
            Some(value) => {
                match value.as_object() {
                    Some(fields_object) => {
                        let fields_result: Result<BTreeMap<String, String>, IoEntityError> = fields_object.iter()
                            .map(|(k, v)| match v.as_str() {
                                Some(string_value) => Ok((k.clone(), String::from(string_value))),
                                None => Err(IoEntityError::Read(format!("Value {} is not a string", v))),
                            })
                            .collect();
                        let fields = try!(fields_result);
                        Ok(EntityDescriptionView { fields: fields })
                    }
                    None => Err(IoEntityError::Read(String::from("Property fields is not object"))),
                }
            }
            None => Err(IoEntityError::Read(String::from("Property fields not found"))),
        }
    }
}

impl TableDescriptionView {
    pub fn from_json(json: &rustless::json::JsonValue) -> Result<TableDescriptionView, IoEntityError> {
        let name = try!(json.find("name").and_then(|name| name.as_str()).ok_or(IoEntityError::Read(String::from("Table name not found"))));
        trace!("Reading table with name = {}", name);
        let key = try!(EntityDescriptionView::from_json(json.find("key").unwrap().as_object().unwrap()));
        let value = try!(EntityDescriptionView::from_json(json.find("value").unwrap().as_object().unwrap()));
        trace!("Table description {} succefully readed", name);
        Ok(TableDescriptionView {
            name: String::from(name),
            key: key,
            value: value,
        })
    }
}

// EntityDescription impl
impl ToJson for EntityDescription {
    fn to_json(&self) -> rustless::json::JsonValue {
        rustless::json::JsonValue::Object(self.fields
            .iter()
            .map(|(k, v)| (k.clone(), v.to_json()))
            .collect())
    }
}

impl EntityDescription {
    pub fn get_field_id(&self, name: &String) -> Option<&u16> {
        self.reverse_ids_map.get(name)
    }

    pub fn get_filed_name(&self, id: &u16) -> Option<&String> {
        self.ids_map.get(id)
    }

    pub fn get_field(&self, name: &String) -> Option<Arc<Box<TypeDescription>>> {
        self.fields.get(name).map(|field| field.clone())
    }

    fn from_view(view: &EntityDescriptionView, type_descs: &BTreeMap<String, Arc<Box<TypeDescription>>>) -> Result<EntityDescription, String> {
        let mut entity_fields = view.fields
            .iter()
            .map(|(k, v)| {
                (k.clone(),
                 type_descs.get(v)
                     .map(|type_desc| type_desc.clone())
                     .ok_or(v.clone()))
            })
            .collect();

        let undefined_fields: Vec<String> = EntityDescription::get_undefined_fields(&entity_fields);
        match undefined_fields.iter().next() {
            Some(first) => {
                let undefined_fields_str = undefined_fields.iter()
                    .skip(1)
                    .fold(first.clone(),
                          |base, field_name| base + ", " + field_name.as_str());
                Err(format!("For next fields not found type descriptions: {}",
                            undefined_fields_str))
            }
            None => {
                let entity_fields = entity_fields.iter_mut()
                    .filter_map(move |(k, v)| v.clone().ok().map(|value| (k.clone(), value)))
                    .collect();
                Ok(EntityDescription::from_fields(entity_fields))
            }
        }
    }

    fn get_undefined_fields(entity_fields: &BTreeMap<String, Result<Arc<Box<TypeDescription>>, String>>) -> Vec<String> {
        entity_fields.iter()
            .filter_map(|(k, v)| {
                match *v {
                    Ok(_) => None,
                    Err(ref error) => Some(format!("{}: {}", k, error)),
                }
            })
            .collect::<Vec<String>>()
    }

    fn blank() -> EntityDescription {
        EntityDescription {
            count: AtomicUsize::new(0),
            fields: BTreeMap::new(),
            ids_map: BTreeMap::new(),
            reverse_ids_map: BTreeMap::new(),
        }
    }

    fn from_fields(fields: BTreeMap<String, Arc<Box<TypeDescription>>>) -> EntityDescription {
        let count = AtomicUsize::new(0);
        let ids_map = fields.iter()
            .map(|(k, _)| (count.fetch_add(1, Ordering::Relaxed) as u16, k.clone()))
            .collect::<BTreeMap<u16, String>>(); //BTreeMap::<u16, String>::new();
        let reverse_ids_map = ids_map.iter().map(|(k, v)| (v.clone(), k.clone())).collect::<BTreeMap<String, u16>>();
        EntityDescription {
            count: count,
            fields: fields,
            ids_map: ids_map,
            reverse_ids_map: reverse_ids_map,
        }
    }

    fn add_field(&mut self, name: String, type_desc: TypeDescription) {
        self.fields.insert(name.clone(), Arc::new(Box::new(type_desc)));
        let id = self.count.fetch_add(1, Ordering::Relaxed) as u16;
        self.ids_map.insert(id.clone(), name.clone());
        self.reverse_ids_map.insert(name.clone(), id.clone());
    }
}

// TableDescription impl
impl ToJson for TableDescription {
    fn to_json(&self) -> rustless::json::JsonValue {
        let mut res = BTreeMap::<String, rustless::json::JsonValue>::new();
        res.insert(String::from("name"),
                   rustless::json::to_value(self.name.clone()));
        res.insert(String::from("key"), self.key.to_json());
        res.insert(String::from("value"), self.value.to_json());
        rustless::json::JsonValue::Object(res)
    }
}

impl TableDescription {
    pub fn from_view(view: &TableDescriptionView, type_descs: &BTreeMap<String, Arc<Box<TypeDescription>>>) -> Result<TableDescription, String> {
        let key_desc = try!(EntityDescription::from_view(&view.key, type_descs).map_err(|error| format!("Cannot read key description: {}", error)));
        let value_desc = try!(EntityDescription::from_view(&view.value, type_descs)
            .map_err(|error| format!("Cannot read value description: {}", error)));
        Ok(TableDescription {
            name: view.name.clone(),
            key: key_desc,
            value: value_desc,
        })
    }
}
