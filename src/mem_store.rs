use std::collections::BTreeSet;

use crate::{
    persist::Backend,
    store::{Builtins, Store, AVE, EAV},
    Entity, Result, Value,
};

#[derive(PartialEq, Debug, Clone)]
pub struct MemStore {
    eav: BTreeSet<EAV>,
    ave: BTreeSet<AVE>,
    builtins: Builtins,
    next_id: u64,
    next_tx: u64,
}

impl MemStore {
    pub fn new() -> Self {
        let mut s = MemStore {
            eav: BTreeSet::new(),
            ave: BTreeSet::new(),
            builtins: Builtins::default(),
            next_id: 0,
            next_tx: 1,
        };

        s.builtins = Builtins::initialize(&mut s);

        s
    }

    pub fn save<B: Backend>(&self, mut backend: B) -> Result<B> {
        backend.save(self.iter())
    }

    pub fn load<B: Backend>(mut backend: B) -> Result<Self>
    where
        Self: Sized,
    {
        let mut s = Self::new();

        let mut ident = Entity(0);
        for eav in backend.load()? {
            if eav.e == eav.a && eav.v == Value::Str("db/ident".to_string()) {
                ident = eav.e;
            }
            s.insert_raw(eav.e, eav.a, eav.v);
        }

        // Need this to recover builtin entities
        s.builtins = Builtins {
            ident,
            ..Default::default()
        };

        if let Some(last) = s.eav.last() {
            s.next_id = last.e.0 + 1;
        }

        macro_rules! recover_builtin {
            ($ident:literal) => {
                match s.get_entity_by_ident($ident) {
                    Some(e) => e,
                    None => {
                        return Err(format!(
                            "Could not find built-in entity `{}`. DB corrupt?",
                            $ident
                        ))
                    }
                }
            };
        }

        s.builtins = Builtins {
            ident,
            type_: recover_builtin!("db/type"),
            card: recover_builtin!("db/cardinality"),
            doc: recover_builtin!("db/doc"),
            one: recover_builtin!("db.cardinality/one"),
            many: recover_builtin!("db.cardinality/many"),
            ref_: recover_builtin!("db.type/ref"),
            string: recover_builtin!("db.type/string"),
            int: recover_builtin!("db.type/int"),
            float: recover_builtin!("db.type/float"),
            bool: recover_builtin!("db.type/bool"),
        };

        Ok(s)
    }
}

impl Store for MemStore {
    fn builtins(&self) -> &Builtins {
        &self.builtins
    }

    fn fresh_entity_id(&mut self) -> Entity {
        let result = Entity(self.next_id);
        self.next_id += 1;
        result
    }

    fn insert_raw(&mut self, e: Entity, a: Entity, v: impl Clone + Into<Value>) {
        self.eav.insert(EAV {
            e,
            a,
            v: v.clone().into(),
        });
        self.ave.insert(AVE { a, v: v.into(), e });
        self.next_id = self.next_id.max(e.0 + 1);
    }

    fn retract_raw(&mut self, e: Entity, a: Entity, v: Value) {
        self.eav.remove(&EAV { e, a, v: v.clone() });
        self.ave.remove(&AVE { a, v, e });
    }

    fn iter(&self) -> impl Iterator<Item = EAV> + '_ {
        self.eav.iter().cloned()
    }

    fn iter_entity(&self, e: Entity) -> impl Iterator<Item = EAV> + '_ {
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

    fn iter_entity_attribute(&self, e: Entity, a: Entity) -> impl Iterator<Item = EAV> + '_ {
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

    fn iter_attribute_value(&self, a: Entity, v: Value) -> impl Iterator<Item = EAV> + '_ {
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
}
