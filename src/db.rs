// MIT License
//
// Copyright (c) 2019 Gregory Meyer
//
// Permission is hereby granted, free of charge, to any person
// obtaining a copy of this software and associated documentation files
// (the "Software"), to deal in the Software without restriction,
// including without limitation the rights to use, copy, modify, merge,
// publish, distribute, sublicense, and/or sell copies of the Software,
// and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
// BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
// ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

use crate::{resp::RespData, sync::{RwLock, RwLockRead, RwLockWrite}};

use std::{collections::VecDeque, mem, str};

use hashbrown::{HashMap, hash_map::Entry};
use tokio::prelude::{*, future::*};

type DatabaseMap = HashMap<Vec<u8>, RwLock<Value>>;
type DatabaseInner = RwLock<DatabaseMap>;

#[derive(Clone)]
pub struct Database {
    inner: DatabaseInner,
}

impl Database {
    pub fn new() -> Database {
        Database{inner: DatabaseInner::new(DatabaseMap::new())}
    }

    pub fn decr(&self, key: Vec<u8>) -> impl RespFuture {
        self.decrby(key, 1)
    }

    pub fn decrby(&self, key: Vec<u8>, decrement: i64) -> impl RespFuture {
        self.rmw_integer_or_else(key, move |i| i - decrement, move || -decrement)
    }

    pub fn get(&self, key: Vec<u8>) -> impl RespFuture {
        self.bucket(key)
            .and_then(|bucket| bucket.read())
            .map(|bucket| {
                if let Value::String(ref s) = *bucket {
                    RespData::BulkString(s.clone())
                } else {
                    err_wrong_type()
                }
            })
            .or_else(|_| Ok(RespData::Nil))
    }

    pub fn getset(&self, key: Vec<u8>, value: Vec<u8>) -> impl RespFuture {
        let other_value = value.clone();

        self.bucket_or_else(key, move || Value::String(other_value))
            .and_then(move |(bucket, inserted)| {
                if inserted {
                    Either::A(future::ok::<RespData, ()>(RespData::Nil))
                } else {
                    let inserted = bucket
                        .write()
                        .map(move |mut guard| {
                            if let Value::String(ref mut s) = *guard {
                                let mut prev_value = value;
                                mem::swap(&mut prev_value, s);

                                RespData::BulkString(prev_value)
                            } else {
                                err_wrong_type()
                            }
                        });

                    Either::B(inserted)
                }
            })
    }

    pub fn incr(&self, key: Vec<u8>) -> impl RespFuture {
        self.incrby(key, 1)
    }

    pub fn incrby(&self, key: Vec<u8>, increment: i64) -> impl RespFuture {
        self.rmw_integer_or_else(key, move |i| i + increment, move || increment)
    }

    pub fn set(&self, key: Vec<u8>, value: Vec<u8>) -> impl RespFuture {
        let other_value = value.clone();

        self.bucket_or_else(key, move || Value::String(other_value))
            .and_then(move |(bucket, inserted)| {
                if inserted {
                    Either::A(future::ok::<RespData, ()>(RespData::ok()))
                } else {
                    let inserted = bucket
                        .write()
                        .map(move |mut guard| {
                            *guard = Value::String(value);

                            RespData::ok()
                        });

                    Either::B(inserted)
                }
            })
    }

    fn rmw_integer_or_else<F: FnOnce(i64) -> i64, G: FnOnce() -> i64>(&self, key: Vec<u8>, f: F, or_else: G) -> impl RespFuture {
        self
            .bucket_or_else(key, || Value::String(Database::stringify(or_else())))
            .and_then(move |(bucket, inserted)| {
                if inserted {
                    Either::A(future::ok::<RespData, ()>(RespData::ok()))
                } else {
                    let mapped = bucket
                        .write()
                        .map(move |mut guard| {
                            if let Ok(int) = guard.as_int() {
                                let modified = f(int);
                                *guard = Value::String(Database::stringify(modified));

                                RespData::ok()
                            } else {
                                err_not_an_integer()
                            }
                        });

                    Either::B(mapped)
                }
            })
    }

    fn bucket(&self, key: Vec<u8>) -> Bucket {
        Bucket{read: self.inner.read(), key}
    }

    fn bucket_or_else<F: FnOnce() -> Value>(&self, key: Vec<u8>, or_else: F) -> BucketOrElse<F> {
        BucketOrElse{write: self.inner.write(), key: Some(key), or_else: Some(or_else)}
    }

    fn stringify(value: i64) -> Vec<u8> {
        value.to_string().into_bytes()
    }
}

pub trait RespFuture: Future<Item=RespData, Error=()> { }

impl<F: Future<Item=RespData, Error=()>> RespFuture for F { }

struct Bucket {
    read: RwLockRead<DatabaseMap>,
    key: Vec<u8>,
}

impl Future for Bucket {
    type Item = RwLock<Value>;
    type Error = ();

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        if let Ok(Async::Ready(guard)) = self.read.poll() {
            if let Some(value) = guard.get(&self.key) {
                Ok(Async::Ready(value.clone()))
            } else {
                Err(())
            }
        } else {
            Ok(Async::NotReady)
        }
    }
}

struct BucketOrElse<F: FnOnce() -> Value> {
    write: RwLockWrite<DatabaseMap>,
    key: Option<Vec<u8>>,
    or_else: Option<F>,
}

impl<F: FnOnce() -> Value> Future for BucketOrElse<F> {
    type Item = (RwLock<Value>, bool);
    type Error = ();

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        if let Ok(Async::Ready(mut guard)) = self.write.poll() {
            let default_fn = self.or_else.take().unwrap();
            let key = self.key.take().unwrap();

            let ret = match guard.entry(key) {
                Entry::Occupied(e) => (e.get().clone(), false),
                Entry::Vacant(e) => (e.insert(RwLock::new(default_fn())).clone(), true),
            };

            Ok(Async::Ready(ret))
        } else {
            Ok(Async::NotReady)
        }
    }
}

enum Value {
    String(Vec<u8>),
    List(VecDeque<Vec<u8>>)
}

impl Value {
    fn as_int(&self) -> Result<i64, RespData> {
        if let Value::String(s) = self {
            if let Ok(utf8) = str::from_utf8(&s) {
                if let Ok(i) = utf8.parse() {
                    return Ok(i);
                }
            }
        }

        return Err(err_not_an_integer());
    }
}

fn err_not_an_integer() -> RespData {
    RespData::Error("ERR value is not an integer or out of range".into())
}

fn err_wrong_type() -> RespData {
    RespData::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into())
}
