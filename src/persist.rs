use serde::Deserialize;

use crate::{store::EAV, Entity, Result, Value};

pub trait Backend: Sized {
    fn save(&mut self, data: impl Iterator<Item = EAV>) -> Result<Self>;
    fn load(&mut self) -> Result<impl Iterator<Item = EAV>>;
}

#[derive(Debug)]
pub struct Json(serde_json::Value);

impl Json {
    pub fn new() -> Self {
        Json(serde_json::Value::Null)
    }

    pub fn extract(self) -> serde_json::Value {
        self.0
    }
}

impl Backend for Json {
    fn save(&mut self, data: impl Iterator<Item = EAV>) -> Result<Self> {
        let mut entries = vec![];

        for eav in data {
            entries.push(serde_json::json!([eav.e.0, eav.a.0, eav.v]));
        }

        Ok(Json(serde_json::Value::Array(entries)))
    }

    fn load(&mut self) -> Result<impl Iterator<Item = EAV>> {
        #[derive(Deserialize)]
        struct Triple {
            e: u64,
            a: u64,
            v: Value,
        }

        let deserialized: Vec<Triple> =
            serde_json::from_value(self.0.clone()).map_err(|e| e.to_string())?;

        Ok(deserialized.into_iter().map(|triple| EAV {
            e: Entity(triple.e),
            a: Entity(triple.a),
            v: triple.v,
        }))
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use crate::{
        movies::DATA,
        store::{MemStore, Store},
    };

    use super::Json;

    #[test]
    fn json_roundtrip() {
        let serialized = DATA.save(Json::new()).unwrap();
        let recovered = MemStore::load(serialized).unwrap();
        assert_eq!(recovered, *DATA);
    }

    #[test]
    fn json_fixture() {
        let actual = DATA.save(Json::new()).unwrap().extract();
        let expected = serde_json::json!([
            [0, 0, { "str": "db/ident" }],
            [0, 1, { "ref": 7 }],
            [0, 2, { "ref": 4 }],
            [0, 3, { "str": "The identifier of an attribute" }],
            [1, 0, { "str": "db/type" }],
            [1, 1, { "ref": 6 }],
            [1, 2, { "ref": 4 }],
            [1, 3, { "str": "Type of an attribute" }],
            [2, 0, { "str": "db/cardinality" }],
            [2, 1, { "ref": 6 }],
            [2, 2, { "ref": 4 }],
            [2, 3, { "str": "Cardinality of an attribute" }],
            [3, 0, { "str": "db/doc" }],
            [3, 1, { "ref": 7 }],
            [3, 2, { "ref": 4 }],
            [3, 3, { "str": "Documentation string for an attribute" }],
            [4, 0, { "str": "db.cardinality/one" }],
            [5, 0, { "str": "db.cardinality/many" }],
            [6, 0, { "str": "db.type/ref" }],
            [7, 0, { "str": "db.type/string" }],
            [8, 0, { "str": "db.type/int" }],
            [9, 0, { "str": "db.type/float" }],
            [10, 0, { "str": "db.type/bool" }],
            [11, 0, { "str": "name" }],
            [11, 1, { "ref": 7 }],
            [11, 2, { "ref": 4 }],
            [11, 3, { "str": "The name" }],
            [12, 0, { "str": "age" }],
            [12, 1, { "ref": 8 }],
            [12, 2, { "ref": 4 }],
            [12, 3, { "str": "" }],
            [13, 0, { "str": "friend" }],
            [13, 1, { "ref": 6 }],
            [13, 2, { "ref": 5 }],
            [13, 3, { "str": "Friend" }],
            [100, 11, { "str": "Moritz" }],
            [100, 12, { "int": 39 }],
            [150, 11, { "str": "Moritz" }],
            [150, 12, { "int": 30 }],
            [200, 11, { "str": "Piet" }],
            [200, 12, { "int": 39 }],
        ]);
        assert_eq!(actual, expected);
    }
}
