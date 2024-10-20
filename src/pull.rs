use std::collections::HashMap;

use crate::{store::Store, Attribute, Entity, Result, Value};

#[derive(PartialEq, Debug)]
enum PullValue {
    Missing,
    Lit(Value),
    List(Vec<PullValue>),
    Record(HashMap<Attribute, PullValue>),
}

// (pull ?e [:name :age {:friend [:name :age]}])

enum Api {
    Return,
    List(Vec<Api>),
    In(Attribute, Box<Api>),
    Back(Attribute, Box<Api>),
}

impl Api {
    pub fn pull(&self, start: &Value, store: &Store) -> Result<PullValue> {
        match self {
            Self::Return => Ok(PullValue::Lit(start.clone())),
            Self::List(list) => {
                let mut record = HashMap::new();
                for sub in list {
                    if let PullValue::Record(sub_result) = sub.pull(start, store)? {
                        record.extend(sub_result);
                    } else {
                        return Err(format!(
                            "Internal error: pull result should be PullValue::Record"
                        ));
                    }
                }
                Ok(PullValue::Record(record))
            }
            Self::In(a, sub) => {
                if let Value::Ref(e) = start {
                    if let Some(a_e) = store.get_attribute_id(a) {
                        let values: Vec<Value> = store
                            .iter_entity_attribute(Entity(*e), a_e)
                            .map(|eav| eav.v)
                            .collect();

                        let value = match values.len() {
                            0 => PullValue::Missing,
                            1 => sub.pull(&values[0], store)?,
                            _ => PullValue::List(
                                values
                                    .into_iter()
                                    .map(|v| sub.pull(&v, store))
                                    .collect::<Result<Vec<_>>>()?,
                            ),
                        };

                        let mut record = HashMap::new();
                        record.insert(a.clone(), value);
                        Ok(PullValue::Record(record))
                    } else {
                        Err(format!("Attribute `{}` does not exist", a))
                    }
                } else {
                    Err(format!(
                        "Can only drill into entity values, got `{}`",
                        start
                    ))
                }
            }
            Self::Back(a, sub) => {
                if let Some(a_e) = store.get_attribute_id(a) {
                    let values: Vec<Entity> = store
                        .iter_attribute_value(a_e, start.clone())
                        .map(|eav| eav.e)
                        .collect();

                    let value = match values.len() {
                        0 => PullValue::Missing,
                        1 => sub.pull(&Value::Ref(values[0].0), store)?,
                        _ => PullValue::List(
                            values
                                .into_iter()
                                .map(|v| sub.pull(&Value::Ref(v.0), store))
                                .collect::<Result<Vec<_>>>()?,
                        ),
                    };

                    let mut record = HashMap::new();
                    record.insert(a.clone(), value);
                    Ok(PullValue::Record(record))
                } else {
                    Err(format!("Attribute `{}` does not exist", a))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use crate::{movies::STORE, pull::PullValue, Attribute, Value};

    use super::Api;

    #[test]
    fn pull() {
        let api = Api::List(vec![
            Api::In(Attribute("movie/title".to_string()), Box::new(Api::Return)),
            Api::In(
                Attribute("movie/cast".to_string()),
                Box::new(Api::List(vec![
                    Api::In(Attribute("person/name".to_string()), Box::new(Api::Return)),
                    Api::Back(
                        Attribute("movie/cast".to_string()),
                        Box::new(Api::In(
                            Attribute("movie/title".to_string()),
                            Box::new(Api::Return),
                        )),
                    ),
                ])),
            ),
        ]);

        let result = api.pull(&Value::Ref(202), &STORE).unwrap();
        assert_eq!(result, PullValue::Missing);
    }
}
