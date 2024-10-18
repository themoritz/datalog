use crate::{store::EAV, Result};

pub trait Backend: Sized {
    fn save<'a>(&mut self, data: impl Iterator<Item = &'a EAV>) -> Result<Self>;
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
    fn save<'a>(&mut self, data: impl Iterator<Item = &'a EAV>) -> Result<Self> {
        let mut entries = vec![];

        for eav in data {
            entries.push(serde_json::json!([eav.e.0, eav.a.0, eav.v]));
        }

        Ok(Json(serde_json::Value::Array(entries)))
    }

    fn load(&mut self) -> Result<impl Iterator<Item = EAV>> {
        Ok(Vec::<EAV>::new().into_iter())
    }
}
