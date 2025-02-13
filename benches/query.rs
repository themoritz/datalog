use std::{collections::HashSet, hint::black_box};

use criterion::{criterion_group, criterion_main, Criterion};

use datalog::{
    add,
    movies::{DATA, STORE},
    pull,
    pull::PullValue,
    query,
    store::{MemStore, Store},
    transact::Tmp,
    Attribute, Value,
};

fn run_query(name: &str) -> HashSet<Vec<Value>> {
    let q = query! {
        find: [?director, ?movie],
        where: [
            (?a, "person/name" = name),
            (?m, "movie/cast" = ?a),
            (?m, "movie/title" = ?movie),
            (?m, "movie/director" = ?d),
            (?d, "person/name" = ?director)
        ]
    };

    let store: &MemStore = &STORE;
    q.qeval(store).unwrap()
}

fn run_pull(e: u64) -> PullValue {
    let api = pull!({
        "movie/title",
        "movie/cast": {
            "person/name",
            <- "movie/cast": {
                "movie/title"
            }
        }
    });

    let store: &MemStore = &STORE;
    api.pull(&Value::Ref(e), store).unwrap()
}

fn run_tx(e: &str) {
    let mut store: MemStore = (*DATA).clone();

    let tx = add!(Tmp(e), {
        "name": "Tom",
        "age": 5,
        "friend": {
            "name": "Sofie",
            "age": 7
        }
    });

    store.transact(tx).unwrap();
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("arnold", |b| {
        b.iter(|| run_query(black_box("Arnold Schwarzenegger")))
    });
    c.bench_function("pull", |b| b.iter(|| run_pull(black_box(202))));
    c.bench_function("transact", |b| b.iter(|| run_tx(black_box("a"))));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
