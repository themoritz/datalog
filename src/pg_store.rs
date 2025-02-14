use tokio::runtime::Runtime;

use crate::{
    store::{Builtins, Store},
    Entity,
};

struct PgStore {
    rt: tokio::runtime::Runtime,
    pool: sqlx::PgPool,
    builtins: Builtins,
    user_id: i32,
}

impl PgStore {
    fn new(user_id: i32, pool: sqlx::PgPool, rt: Runtime, initialize: bool) -> Self {
        // let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
        //     let def = "postgres://localhost/postgres".to_string();
        //     println!("DATABASE_URL not set, using default `{def}`");
        //     def
        // });
        //let rt = tokio::runtime::Runtime::new().unwrap();
        // let pool = rt.block_on(sqlx::PgPool::connect(&db_url)).unwrap();

        let mut s = PgStore {
            rt,
            pool,
            builtins: Default::default(),
            user_id,
        };

        if initialize {
            s.builtins = Builtins::initialize(&mut s);
        }

        s
    }
}

impl Store for PgStore {
    fn builtins(&self) -> &crate::store::Builtins {
        &self.builtins
    }

    fn fresh_entity_id(&mut self) -> crate::Entity {
        panic!("Not intended to be used")
    }

    fn insert_raw(
        &mut self,
        e: crate::Entity,
        a: crate::Entity,
        v: impl Clone + Into<crate::Value>,
    ) {
        let q = sqlx::query!(
            r#"INSERT INTO triples (user_id, e, a, v) VALUES ($1, $2, $3, $4)"#,
            self.user_id,
            e.0 as i64,
            a.0 as i64,
            serde_json::to_value(v.into()).unwrap(),
        );
        self.rt.block_on(q.execute(&self.pool)).unwrap();
    }

    fn retract_raw(&mut self, e: crate::Entity, a: crate::Entity, v: crate::Value) {
        let q = sqlx::query!(
            r#"DELETE FROM triples WHERE e = $1 AND a = $2 AND v = $3"#,
            e.0 as i64,
            a.0 as i64,
            serde_json::to_value(v).unwrap(),
        );
        self.rt.block_on(q.execute(&self.pool)).unwrap();
    }

    fn iter(&self) -> impl Iterator<Item = crate::store::EAV> + '_ {
        let q = sqlx::query!(r#"SELECT e, a, v FROM triples"#,);
        let rows = self.rt.block_on(q.fetch_all(&self.pool)).unwrap();
        rows.into_iter().map(|row| crate::store::EAV {
            e: Entity(row.e as u64),
            a: Entity(row.a as u64),
            v: serde_json::from_value(row.v).unwrap(),
        })
    }

    fn iter_entity(&self, e: crate::Entity) -> impl Iterator<Item = crate::store::EAV> + '_ {
        let q = sqlx::query!(r#"SELECT e, a, v FROM triples where e = $1"#, e.0 as i64,);
        let rows = self.rt.block_on(q.fetch_all(&self.pool)).unwrap();
        rows.into_iter().map(|row| crate::store::EAV {
            e: Entity(row.e as u64),
            a: Entity(row.a as u64),
            v: serde_json::from_value(row.v).unwrap(),
        })
    }

    fn iter_entity_attribute(
        &self,
        e: crate::Entity,
        a: crate::Entity,
    ) -> impl Iterator<Item = crate::store::EAV> + '_ {
        let q = sqlx::query!(
            r#"SELECT e, a, v FROM triples where e = $1 AND a = $2"#,
            e.0 as i64,
            a.0 as i64,
        );
        let rows = self.rt.block_on(q.fetch_all(&self.pool)).unwrap();
        rows.into_iter().map(|row| crate::store::EAV {
            e: Entity(row.e as u64),
            a: Entity(row.a as u64),
            v: serde_json::from_value(row.v).unwrap(),
        })
    }

    fn iter_attribute_value(
        &self,
        a: crate::Entity,
        v: crate::Value,
    ) -> impl Iterator<Item = crate::store::EAV> + '_ {
        let q = sqlx::query!(
            r#"SELECT e, a, v FROM triples where a = $1 and v = $2"#,
            a.0 as i64,
            serde_json::to_value(v).unwrap(),
        );
        let rows = self.rt.block_on(q.fetch_all(&self.pool)).unwrap();
        rows.into_iter().map(|row| crate::store::EAV {
            e: Entity(row.e as u64),
            a: Entity(row.a as u64),
            v: serde_json::from_value(row.v).unwrap(),
        })
    }
}
