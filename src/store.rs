use std::collections::{BTreeSet, HashSet};

use crate::{
    query::{Entry, Pattern, Where},
    schema::{self, Schema},
    Attribute, Datom, Entity, Result, Value,
};

pub struct Store {
    schema: Schema,
    eav: BTreeSet<EAV>,
    ave: BTreeSet<AVE>,
    next_id: u64,
    next_tx: u64,
}

impl Store {
    pub fn new() -> Self {
        Store {
            schema: Schema::new(),
            eav: BTreeSet::new(),
            ave: BTreeSet::new(),
            next_id: 10,
            next_tx: 1,
        }
    }

    pub fn add_attribute(
        &mut self,
        name: &str,
        type_: schema::Type,
        cardinality: schema::Cardinality,
    ) -> Result<()> {
        let a = Attribute(name.to_string());
        if self.schema.details.contains_key(&a) {
            return Err("Attribute already defined".to_string());
        }

        self.schema
            .attributes
            .insert(Entity(self.next_id), a.clone());

        let details = schema::AttributeDetails { type_, cardinality, id: Entity(self.next_id) };
        self.schema.details.insert(a, details);

        self.next_id += 1;
        Ok(())
    }

    pub fn insert(&mut self, datom: Datom) -> Result<()> {
        let a = match self.schema.get_id(&datom.a) {
            Some(id) => Ok(id),
            None => Err(format!("Could not find id for attribute `{}`", datom.a.0)),
        }?;

        if !self.schema.valid_type(&datom.a, &datom.v) {
            return Err(format!(
                "Invalid type for attribute `{}`: {:?}",
                datom.a.0, datom.v
            ));
        }

        self.eav.insert(EAV {
            e: datom.e,
            a,
            v: datom.v.clone(),
        });
        self.ave.insert(AVE {
            a,
            v: datom.v.clone(),
            e: datom.e,
        });

        Ok(())
    }

    pub fn iter(&self) -> impl Iterator<Item = &EAV> + '_ {
        self.eav.iter()
    }

    pub fn iter_entity(&self, e: Entity) -> impl Iterator<Item = EAV> + '_ {
        let min = EAV {
            e,
            a: Entity::min(),
            v: Value::min(),
        };
        let max = EAV {
            e: e.next(),
            a: Entity::min(),
            v: Value::min(),
        };
        self.eav.range(min..max).map(|eav| eav.clone().into())
    }

    pub fn iter_entity_attribute(&self, e: Entity, a: Entity) -> impl Iterator<Item = EAV> + '_ {
        let min = EAV {
            e,
            a: a.clone(),
            v: Value::min(),
        };
        let max = EAV {
            e,
            a: a.next(),
            v: Value::min(),
        };
        self.eav.range(min..max).map(|eav| eav.clone().into())
    }

    pub fn iter_attribute_value(&self, a: Entity, v: Value) -> impl Iterator<Item = EAV> + '_ {
        let min = AVE {
            a: a.clone(),
            v: v.clone(),
            e: Entity::min(),
        };
        let max = AVE {
            a: a.clone(),
            v: v.next(),
            e: Entity::min(),
        };
        self.ave.range(min..max).map(|ave| ave.clone().into())
    }

    fn resolve_pattern(&self, p: &Pattern<Attribute>) -> Result<Pattern<Entity>> {
        let a = match &p.a {
            Entry::Var(v) => Ok(Entry::Var(v.clone())),
            Entry::Lit(a) => {
                if let Some(id) = self.schema.get_id(&a) {
                    Ok(Entry::Lit(id))
                } else {
                    Err("Id not found".to_string())
                }
            }
        }?;

        Ok(Pattern {
            e: p.e.clone(),
            a,
            v: p.v.clone(),
        })
    }

    pub fn resolve_where(&self, whr: &Where<Attribute>) -> Result<Where<Entity>> {
        match whr {
            Where::Pattern(p) => Ok(Where::Pattern(self.resolve_pattern(&p)?)),
            Where::And(l, r) => Ok(Where::and(self.resolve_where(l)?, self.resolve_where(r)?)),
            Where::Or(l, r) => Ok(Where::or(self.resolve_where(l)?, self.resolve_where(r)?)),
        }
    }

    pub fn resolve_result(&self, table: HashSet<Vec<Value>>) -> HashSet<Vec<Value>> {
        table
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|val| {
                        if let Value::Int(i) = val {
                            if let Some(Attribute(a)) = self.schema.attributes.get(&Entity(i)) {
                                Value::Str(a.clone())
                            } else {
                                val
                            }
                        } else {
                            val
                        }
                    })
                    .collect()
            })
            .collect()
    }

    pub fn store_json(&self) -> serde_json::Value {
        let mut entries = vec![];

        let mut attributes: Vec<_> = self.schema.details.iter().collect();
        attributes.sort_by_key(|(_, d)| d.id);

        for (a, details) in attributes {
            entries.push(serde_json::json!([
                details.id.0,
                1,
                a.0
            ]));
            entries.push(serde_json::json!([
                details.id.0,
                2,
                details.type_
            ]));
            entries.push(serde_json::json!([
                details.id.0,
                3,
                details.cardinality
            ]));
        }

        for eav in self.eav.iter() {
            let v = match eav.v {
                Value::Int(i) => serde_json::json!(i),
                Value::Float(f) => serde_json::json!(f),
                Value::Str(ref s) => serde_json::json!(s),
            };
            entries.push(serde_json::json!([
                eav.e.0,
                eav.a.0,
                v
            ]));
        }

        serde_json::Value::Array(entries)
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone)]
pub struct EAV {
    pub e: Entity,
    pub a: Entity,
    pub v: Value,
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone)]
struct AVE {
    a: Entity,
    v: Value,
    e: Entity,
}

impl From<AVE> for EAV {
    fn from(value: AVE) -> Self {
        EAV {
            e: value.e,
            a: value.a,
            v: value.v,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::movies::DATA;

    #[test]
    fn persist_json() {
        let actual = DATA.store_json();
        let expected = serde_json::json!([
            [10, 1, "name"],
            [10, 2, "Str"],
            [10, 3, "One"],

            [11, 1, "age"],
            [11, 2, "Int"],
            [11, 3, "One"],

            [100, 10, "Moritz"],
            [100, 11, 39],

            [150, 10, "Moritz"],
            [150, 11, 30],

            [200, 10, "Piet"],
            [200, 11, 39],
        ]);
        assert_eq!(actual, expected);
    }
}
