# nimdg
In memory database

It is in memory key/value storage with optimistic and pessimistic transactions support with REST api.    
Http api was created via https://github.com/hyperium/hyper library.

## Api

### Values
GET /get/{table_name}/{tx_id}/{key} - get single value by key. tx_id - opened transaction. Key can be composable value, examples:
```
1
"string"
{ "name": "John", "lastname": "Doe }
```
GET /get/{table_name}/{tx_id}/{start}/{count} - get range of values from start to count . 
POST /put/{table_name}/{tx_id}/{key} - put new value. Body must be json representation of inserted value . 
DELETE /delete/{table_name}/{tx_td}/{key} - delete value by specified key . 

### Tables
GET /info - get description of all tables . 
POST /meta/table - create new table. Example:  
```
{
        "name": "Times",
        "key": {
            "fields": {
                "id": {"type_name": "u64"}
            }
        },
        "value": {
            "fields": {
                "date": {"type_name": "date"}
                "date_time": {"type_name": "date_time"}
             }
        }
    }
```

GET /meta/table/{name}  - get info about table with specified name . 

### Transactions
GET /meta/tx/list - list of runned transactions . 
DELETE /tx/stop/{tx_id} - commit specified transaction . 
DELETE /tx/rollback/{tx_id} - rollback specified transaction . 
POST /tx/{mode}/start - start new transaction. Mode = optimistic | pessimistic . 
