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

use crate::resp::RespData;

use std::{collections::VecDeque, mem, sync::Arc};

use hashbrown::{hash_map::Entry, HashMap, HashSet};
use lock_api::RwLockUpgradableReadGuard;
use parking_lot::RwLock;

pub enum Value {
    String(String),
    List(VecDeque<String>),
    Set(HashSet<String>),
    Hash(HashMap<String, String>),
}

impl Value {
    fn new(value: Value) -> Arc<RwLock<Value>> {
        Arc::new(RwLock::new(value))
    }
}

pub struct KeyValueStore {
    map: RwLock<HashMap<String, Arc<RwLock<Value>>>>,
}

impl KeyValueStore {
    pub fn new() -> KeyValueStore {
        KeyValueStore {
            map: RwLock::new(HashMap::new()),
        }
    }

    pub fn decr(&self, key: String) -> RespData {
        self.decrby(key, 1)
    }

    pub fn decrby(&self, key: String, decrement: i64) -> RespData {
        self.rmw_integer(key, |x| x - decrement, || -decrement)
    }

    pub fn get(&self, key: &str) -> RespData {
        let bucket_ptr = {
            let map = self.map.read();

            if let Some(v) = map.get(key) {
                v.clone()
            } else {
                return RespData::Null;
            }
        };

        let bucket = bucket_ptr.read();

        match &*bucket {
            Value::String(s) => RespData::BulkString(s.clone()),
            _ => KeyValueStore::wrongtype(),
        }
    }

    pub fn getset(&self, key: String, mut value: String) -> RespData {
        let bucket_ptr = {
            let map = self.map.upgradable_read();

            if let Some(v) = map.get(&key) {
                v.clone()
            } else {
                let mut writer = RwLockUpgradableReadGuard::upgrade(map);

                match writer.entry(key) {
                    Entry::Occupied(e) => e.get().clone(),
                    Entry::Vacant(e) => {
                        e.insert(Value::new(Value::String(value)));

                        return RespData::Null;
                    }
                }
            }
        };

        let mut bucket = bucket_ptr.write();

        match &mut *bucket {
            Value::String(s) => {
                mem::swap(s, &mut value);

                RespData::BulkString(value)
            }
            _ => KeyValueStore::wrongtype(),
        }
    }

    pub fn incr(&self, key: String) -> RespData {
        self.incrby(key, 1)
    }

    pub fn incrby(&self, key: String, increment: i64) -> RespData {
        self.rmw_integer(key, |x| x + increment, || increment)
    }

    pub fn mget(&self, keys: &[&str]) -> RespData {
        let maybe_bucket_ptrs: Vec<_> = {
            let map = self.map.read();

            keys.iter()
                .map(|k| map.get(*k).map(|v| v.clone()))
                .collect()
        };

        RespData::Array({
            maybe_bucket_ptrs
                .iter()
                .map(|maybe_bucket_ptr| {
                    if let Some(bucket_ptr) = maybe_bucket_ptr {
                        let bucket = bucket_ptr.read();

                        if let Value::String(s) = &*bucket {
                            RespData::BulkString(s.clone())
                        } else {
                            RespData::Null
                        }
                    } else {
                        RespData::Null
                    }
                })
                .collect()
        })
    }

    pub fn set(&self, key: String, value: String) -> RespData {
        let bucket_ptr = {
            let map = self.map.upgradable_read();

            if let Some(v) = map.get(&key) {
                v.clone()
            } else {
                let mut writer = RwLockUpgradableReadGuard::upgrade(map);

                match writer.entry(key) {
                    Entry::Occupied(e) => e.get().clone(),
                    Entry::Vacant(e) => {
                        e.insert(Value::new(Value::String(value)));

                        return KeyValueStore::ok();
                    }
                }
            }
        };

        let mut bucket = bucket_ptr.write();

        match &mut *bucket {
            Value::String(s) => *s = value,
            _ => *bucket = Value::String(value),
        }

        KeyValueStore::ok()
    }

    pub fn setnx(&self, key: String, value: String) -> RespData {
        let map = self.map.upgradable_read();

        if let Some(_) = map.get(&key) {
            return RespData::Integer(0);
        }

        let mut writer = RwLockUpgradableReadGuard::upgrade(map);

        match writer.entry(key) {
            Entry::Occupied(_) => RespData::Integer(0),
            Entry::Vacant(e) => {
                e.insert(Value::new(Value::String(value)));

                RespData::Integer(1)
            }
        }
    }

    fn ok() -> RespData {
        RespData::SimpleString("OK".to_string())
    }

    fn wrongtype() -> RespData {
        RespData::Error(
            "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
        )
    }

    fn rmw_integer<F: FnOnce(i64) -> i64, G: FnOnce() -> i64>(
        &self,
        key: String,
        if_present: F,
        if_absent: G,
    ) -> RespData {
        let bucket_ptr = {
            let map = self.map.upgradable_read();

            if let Some(v) = map.get(&key) {
                v.clone()
            } else {
                let mut writer = RwLockUpgradableReadGuard::upgrade(map);

                match writer.entry(key) {
                    Entry::Occupied(e) => e.get().clone(),
                    Entry::Vacant(e) => {
                        let val = if_absent();
                        e.insert(Value::new(Value::String(format!("{}", val))));

                        return RespData::Integer(val);
                    }
                }
            }
        };

        let mut bucket = bucket_ptr.write();

        match &mut *bucket {
            Value::String(s) => {
                if let Ok(i) = s.parse::<i64>().map(if_present) {
                    *s = format!("{}", i);

                    RespData::Integer(i)
                } else {
                    RespData::Error("ERR value is not an integer or out of range".to_string())
                }
            }
            _ => KeyValueStore::wrongtype(),
        }
    }
}
