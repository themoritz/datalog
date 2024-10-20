use std::{collections::HashMap, error::Error, fmt::Display};

use serde::{
    de::{self, DeserializeSeed, MapAccess, SeqAccess},
    forward_to_deserialize_any, Deserializer,
};

use crate::{store::Store, Attribute, Entity, Result, Value};

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

                        // TODO: Need to look at cardinality
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

                    // TODO: Look at cardinality!
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

#[derive(PartialEq, Debug)]
enum PullValue {
    Missing,
    Lit(Value),
    List(Vec<PullValue>),
    Record(HashMap<Attribute, PullValue>),
}

#[derive(Debug)]
pub enum PullError {
    Message(String),
}

impl Display for PullError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PullError::Message(m) => m.fmt(f),
        }
    }
}

impl Error for PullError {}

impl de::Error for PullError {
    fn custom<T>(msg: T) -> Self
    where
        T: std::fmt::Display,
    {
        PullError::Message(msg.to_string())
    }
}

impl<'de> Deserializer<'de> for &'de PullValue {
    type Error = PullError;

    fn deserialize_any<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        match self {
            PullValue::Missing => visitor.visit_none(),
            PullValue::Lit(value) => match value {
                Value::Ref(e) => visitor.visit_u64(*e),
                Value::Bool(b) => visitor.visit_bool(*b),
                Value::Float(f) => visitor.visit_f64(f.into_inner()),
                Value::Int(i) => visitor.visit_u64(*i),
                Value::Str(s) => visitor.visit_string(s.clone()),
            },
            PullValue::List(list) => visitor.visit_seq(PullValueSeqAccess {
                iter: list.iter(),
                len: list.len(),
            }),
            PullValue::Record(record) => visitor.visit_map(PullValueMapAccess {
                iter: record.iter(),
                value: None,
                len: record.len(),
            }),
        }
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes byte_buf option unit
        unit_struct newtype_struct seq tuple tuple_struct map struct enum identifier ignored_any
    }
}

struct PullValueSeqAccess<'a> {
    iter: std::slice::Iter<'a, PullValue>,
    len: usize,
}

impl<'de> SeqAccess<'de> for PullValueSeqAccess<'de> {
    type Error = PullError;

    fn next_element_seed<T>(
        &mut self,
        seed: T,
    ) -> std::result::Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        if let Some(value) = self.iter.next() {
            self.len -= 1;
            seed.deserialize(value).map(Some)
        } else {
            Ok(None)
        }
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.len)
    }
}

struct PullValueMapAccess<'a> {
    iter: std::collections::hash_map::Iter<'a, Attribute, PullValue>,
    value: Option<&'a PullValue>,
    len: usize,
}

impl<'de> MapAccess<'de> for PullValueMapAccess<'de> {
    type Error = PullError;

    fn next_key_seed<K>(&mut self, seed: K) -> std::result::Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        if let Some((key, value)) = self.iter.next() {
            self.value = Some(value);
            self.len -= 1;
            seed.deserialize(key).map(Some)
        } else {
            self.value = None;
            Ok(None)
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        if let Some(value) = self.value.take() {
            seed.deserialize(value)
        } else {
            Err(de::Error::custom("Value is missing"))
        }
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.len)
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde::Deserialize;

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

        #[derive(Deserialize, Debug, PartialEq)]
        struct MovieWithCast {
            #[serde(rename = "movie/title")]
            title: String,
            #[serde(rename = "movie/cast")]
            cast: Vec<Actor>,
        }

        #[derive(Deserialize, Debug, PartialEq)]
        struct Actor {
            #[serde(rename = "person/name")]
            name: String,
            #[serde(rename = "movie/cast")]
            movies: Vec<Movie>,
        }

        #[derive(Deserialize, Debug, PartialEq)]
        struct Movie {
            #[serde(rename = "movie/title")]
            title: String,
        }

        let expected = MovieWithCast {
            title: "Predator".to_string(),
            cast: vec![],
        };

        let result = api.pull(&Value::Ref(202), &STORE).unwrap();
        let deserialized: MovieWithCast = Deserialize::deserialize(&result).unwrap();
        assert_eq!(deserialized, expected);
    }

    #[test]
    fn deserialize_list() {
        let pv = PullValue::List(vec![
            PullValue::Lit(Value::Int(5)),
            PullValue::Lit(Value::Ref(1)),
        ]);
        let result: Vec<u64> = Deserialize::deserialize(&pv).unwrap();
        assert_eq!(result, vec![5, 1]);
    }

    #[test]
    fn deserialize_struct() {
        #[derive(Deserialize, PartialEq, Debug)]
        struct Test {
            x: u64,
            y: String,
        }

        let pv = PullValue::Record(
            vec![
                (Attribute("x".to_string()), PullValue::Lit(Value::Ref(10))),
                (
                    Attribute("y".to_string()),
                    PullValue::Lit(Value::Str("foo".to_string())),
                ),
            ]
            .into_iter()
            .collect(),
        );

        let result: Test = Deserialize::deserialize(&pv).unwrap();
        assert_eq!(
            result,
            Test {
                x: 10,
                y: "foo".to_string()
            }
        );
    }
}
