
extern crate serde;
extern crate serde_json;

use rustless::json::ToJson;
use self::serde::ser::Serializer;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::boxed::Box;
use std::clone::Clone;
use self::serde::de::Deserializer;
use rustless;

use data_base::IoEntityError;
use data_base::MetaManager;

// Type trait, that allow define user type
pub struct TypeDescription {
    pub name: String,
    pub reader: Box<Fn(&rustless::json::JsonValue) -> Result<Vec<u8>, IoEntityError>>,
    pub writer: Box<Fn(&Vec<u8>) -> Result<rustless::json::JsonValue, IoEntityError>>,
}

// Universal description of some entity. For example: key or value
// For performance purposes each field is marked by number id
#[derive(Debug)]
pub struct EntityDescription {
    count: u16,
    pub fields: BTreeMap<String, Arc<Box<FieldDescription>>>, // name -> type_code
    pub ids_map: BTreeMap<u16, String>,
    pub reverse_ids_map: BTreeMap<String, u16>,
}

// impl Serialize for EntityDescription {
// fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
// where S: Serializer
// {
// let mut state = serializer.serialize_struct("EntityDescription", 1)?;
// let fields = self.fields.map(|(k, v)| (k, (*v).clone()));
// state.serialize_field("fields" & fields);
// state.end()
// }
// }
//
// impl<'de> Deserialize<'de> for EntityDescription {
// fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
// where D: Deserializer<'de>
// {
// struct FieldVisitor;
//
// impl<'de> Visitor<'de> for FieldVisitor {
// type Value = EntityDescription;
//
// fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
// formatter.write_str("`secs` or `nanos`")
// }
//
// fn visit_str<E>(self, value: &str) -> Result<Field, E>
// where E: de::Error
// {
// match value {
// "fields" => Ok(EntityDescription::fields),
// _ => Err(de::Error::unknown_field(value, FIELDS)),
// }
// }
// }
//
// deserializer.deserialize_identifier(FieldVisitor)
// }
// }

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FieldDescription {
    pub type_name: String,
}

// Description of table, that is key-value cache
#[derive(Debug)]
pub struct TableDescription {
    pub name: String,
    pub key: EntityDescription,
    pub value: EntityDescription,
}

// For getting from frontend
#[derive(Debug, Serialize, Deserialize)]
pub struct EntityDescriptionView {
    pub fields: BTreeMap<String, FieldDescription>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TableDescriptionView {
    pub name: String,
    pub key: EntityDescriptionView,
    pub value: EntityDescriptionView,
}

// TypeDescription impl
unsafe impl Send for TypeDescription {}
unsafe impl Sync for TypeDescription {}

impl EntityDescriptionView {
    fn field_description_from_json(json: &rustless::json::JsonValue) -> Result<FieldDescription, IoEntityError> {
        match json.as_object() {
            Some(field_description) => {
                field_description.get("type_name")
                    .and_then(|type_name| type_name.as_str())
                    .map(|type_name| FieldDescription { type_name: String::from(type_name) })
                    .ok_or(IoEntityError::Read(String::from("Field type_name in field description not found")))
            }
            None => Err(IoEntityError::Read(String::from("Field description is not a object"))),
        }
    }

    fn from_json(json: &BTreeMap<String, rustless::json::JsonValue>) -> Result<EntityDescriptionView, IoEntityError> {
        match json.get("fields") {
            Some(value) => {
                match value.as_object() {
                    Some(fields_object) => {
                        let fields_result: Result<BTreeMap<String, FieldDescription>, IoEntityError> = fields_object.iter()
                            .map(|(k, v)|
                                EntityDescriptionView::field_description_from_json(v).map(|field_desc| (k.clone(), field_desc))
                             /*match v.as_str() {
                                Some(string_value) => Ok((k.clone(), String::from(string_value))),
                                None => Err(IoEntityError::Read(format!("Value {} is not a string", v))),
                            }*/)
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

impl EntityDescription {
    pub fn get_field_id(&self, name: &String) -> Option<&u16> {
        self.reverse_ids_map.get(name)
    }

    pub fn get_filed_name(&self, id: &u16) -> Option<&String> {
        self.ids_map.get(id)
    }

    pub fn get_field(&self, name: &String) -> Option<Arc<Box<FieldDescription>>> {
        self.fields.get(name).map(|field| field.clone())
    }

    fn from_view(view: &EntityDescriptionView, meta_manager: Arc<MetaManager>) -> Result<EntityDescription, String> {
        let mut entity_fields = view.fields
            .iter()
            .map(|(k, v)| {
                (k.clone(),
                 meta_manager.get_type(&v.type_name)
                     .map(|type_desc| type_desc.clone())
                     .ok_or(v.type_name.clone()))
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
                    .filter_map(move |(k, v)|
                        v.clone().ok().map(|type_desc|
                            (k.clone(),
                            Arc::new(Box::new(FieldDescription { type_name: type_desc.name.clone() })) )))
                    //v.clone().ok().map(|value| (k.clone(), value)))
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
            count: 0,
            fields: BTreeMap::new(),
            ids_map: BTreeMap::new(),
            reverse_ids_map: BTreeMap::new(),
        }
    }

    fn from_fields(fields: BTreeMap<String, Arc<Box<FieldDescription>>>) -> EntityDescription {
        let mut count = 0;
        let ids_map = fields.iter()
            .map(|(k, _)| {
                let res = (count.clone(), k.clone());
                count += 1;
                res
            })
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
        self.fields.insert(name.clone(),
                           Arc::new(Box::new(FieldDescription { type_name: type_desc.name.clone() })));
        let id = self.count;//.fetch_add(1, Ordering::Relaxed) as u16;
        self.count = self.count + 1;
        self.ids_map.insert(id.clone(), name.clone());
        self.reverse_ids_map.insert(name.clone(), id.clone());
    }

    pub fn to_view(&self) -> EntityDescriptionView {
        EntityDescriptionView {
            fields: self.fields
                .iter()
                .map(|(k, v)| {
                    // let box_value: *const Box<FieldDescription> = Arc::into_raw(*v);
                    let t = v.clone();
                    let box_value = &**t;
                    (k.clone(), box_value.clone())
                })
                .collect(),
        }
    }
}

impl ToJson for TableDescription {
    fn to_json(&self) -> rustless::json::JsonValue {
        serde_json::to_value(&self.to_view())
        // let mut res = BTreeMap::<String, rustless::json::JsonValue>::new();
        // res.insert(String::from("name"),
        // rustless::json::to_value(self.name.clone()));
        // res.insert(String::from("key"), self.key.to_json());
        // res.insert(String::from("value"), self.value.to_json());
        // rustless::json::JsonValue::Object(res)
    }
}

impl TableDescription {
    pub fn from_view(view: &TableDescriptionView,
                     meta_manager: Arc<MetaManager> /* type_descs: &BTreeMap<String, Arc<Box<TypeDescription>>> */)
                     -> Result<TableDescription, String> {
        let key_desc = try!(EntityDescription::from_view(&view.key, meta_manager.clone())
            .map_err(|error| format!("Cannot read key description: {}", error)));
        let value_desc = try!(EntityDescription::from_view(&view.value, meta_manager.clone())
            .map_err(|error| format!("Cannot read value description: {}", error)));
        Ok(TableDescription {
            name: view.name.clone(),
            key: key_desc,
            value: value_desc,
        })
    }

    pub fn to_view(&self) -> TableDescriptionView {
        TableDescriptionView {
            name: self.name.clone(),
            key: self.key.to_view(),
            value: self.value.to_view(),
        }
    }
}
