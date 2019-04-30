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

use std::{cmp, collections::VecDeque, mem, sync::Arc};

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

pub struct Database {
    map: RwLock<HashMap<String, Arc<RwLock<Value>>>>,
}

impl Database {
    pub fn new() -> Database {
        Database {
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
                return RespData::Nil;
            }
        };

        let bucket = bucket_ptr.read();

        match &*bucket {
            Value::String(s) => RespData::BulkString(s.clone()),
            _ => Database::wrongtype(),
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
                    Entry::Occupied(_) => unreachable!(), // this should never happen
                    Entry::Vacant(e) => {
                        e.insert(Value::new(Value::String(value)));

                        return RespData::Nil;
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
            _ => Database::wrongtype(),
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
                            RespData::Nil
                        }
                    } else {
                        RespData::Nil
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
                    Entry::Occupied(_) => unreachable!(), // should never happen, upgrade is atomic
                    Entry::Vacant(e) => {
                        e.insert(Value::new(Value::String(value)));

                        return Database::ok();
                    }
                }
            }
        };

        let mut bucket = bucket_ptr.write();

        match &mut *bucket {
            Value::String(s) => *s = value,
            _ => *bucket = Value::String(value),
        }

        Database::ok()
    }

    pub fn setnx(&self, key: String, value: String) -> RespData {
        let map = self.map.upgradable_read();

        if let Some(_) = map.get(&key) {
            return RespData::Integer(0);
        }

        let mut writer = RwLockUpgradableReadGuard::upgrade(map);

        match writer.entry(key) {
            Entry::Occupied(_) => unreachable!(), // should never happen, upgrade is atomic
            Entry::Vacant(e) => {
                e.insert(Value::new(Value::String(value)));

                RespData::Integer(1)
            }
        }
    }

    pub fn lindex(&self, key: &str, index: isize) -> RespData {
        let bucket_ptr = {
            let map = self.map.read();

            if let Some(b) = map.get(key) {
                b.clone()
            } else {
                return RespData::Nil;
            }
        };

        let bucket = bucket_ptr.read();

        if let Value::List(l) = &*bucket {
            let offset = if index < 0 {
                index + l.len() as isize
            } else {
                index
            };

            if offset < 0 || offset as usize >= l.len() {
                RespData::Nil
            } else {
                RespData::BulkString(l[offset as usize].clone())
            }
        } else {
            Database::wrongtype()
        }
    }

    pub fn llen(&self, key: &str) -> RespData {
        let bucket_ptr = {
            let map = self.map.read();

            if let Some(b) = map.get(key) {
                b.clone()
            } else {
                return RespData::Integer(0);
            }
        };

        let bucket = bucket_ptr.read();

        if let Value::List(l) = &*bucket {
            RespData::Integer(l.len() as i64)
        } else {
            Database::wrongtype()
        }
    }

    pub fn lpop(&self, key: &str) -> RespData {
        let bucket_ptr = {
            let map = self.map.read();

            if let Some(b) = map.get(key) {
                b.clone()
            } else {
                return RespData::Nil;
            }
        };

        let mut bucket = bucket_ptr.write();

        if let Value::List(l) = &mut *bucket {
            if let Some(v) = l.pop_front() {
                RespData::BulkString(v)
            } else {
                RespData::Nil
            }
        } else {
            Database::wrongtype()
        }
    }

    pub fn lpush(&self, key: String, value: String) -> RespData {
        let bucket_ptr = {
            let map = self.map.upgradable_read();

            if let Some(v) = map.get(&key) {
                v.clone()
            } else {
                let mut writer = RwLockUpgradableReadGuard::upgrade(map);

                match writer.entry(key) {
                    Entry::Occupied(_) => unreachable!(), // should never happen, upgrade is atomic
                    Entry::Vacant(e) => {
                        let mut list = VecDeque::with_capacity(1);
                        list.push_front(value);

                        e.insert(Value::new(Value::List(list)));

                        return RespData::Integer(1);
                    }
                }
            }
        };

        let mut bucket = bucket_ptr.write();

        if let Value::List(list) = &mut *bucket {
            list.push_front(value);

            RespData::Integer(list.len() as i64)
        } else {
            Database::wrongtype()
        }
    }

    pub fn lrange(&self, key: &str, start: isize, stop: isize) -> RespData {
        let bucket_ptr = {
            let map = self.map.read();

            if let Some(v) = map.get(key) {
                v.clone()
            } else {
                return RespData::Array(Vec::new());
            }
        };

        let bucket = bucket_ptr.read();

        if let Value::List(l) = &*bucket {
            let start_offset = if start < 0 {
                start + l.len() as isize
            } else {
                start
            };

            let stop_offset = if stop < 0 {
                stop + l.len() as isize
            } else {
                stop
            };

            let start_clamped = cmp::max(0, start_offset) as usize;
            let stop_clamped = cmp::min(l.len() as isize, stop_offset) as usize;

            if start_clamped >= l.len() || start_clamped > stop_clamped {
                RespData::Array(Vec::new())
            } else {
                let numel = stop_clamped + 1 - start_clamped;

                let elems = l.iter()
                    .skip(start_clamped)
                    .take(numel)
                    .cloned()
                    .map(RespData::BulkString);

                RespData::Array(elems.collect())
            }
        } else {
            Database::wrongtype()
        }
    }

    pub fn lrem(&self, key: &str, count: isize, value: &str) -> RespData {
        let bucket_ptr = {
            let map = self.map.read();

            if let Some(v) = map.get(key) {
                v.clone()
            } else {
                return RespData::Integer(0);
            }
        };

        let mut bucket = bucket_ptr.write();

        if let Value::List(l) = &mut *bucket {
            if count > 0 {
                let mut new_list = VecDeque::with_capacity(l.len());
                let mut num_removed = 0;

                for elem in l.drain(..) {
                    if num_removed < count && elem == value {
                        num_removed += 1;
                    } else {
                        new_list.push_back(elem);
                    }
                }

                *l = new_list;

                RespData::Integer(num_removed as i64)
            } else if count < 0 {
                let mut new_list = VecDeque::with_capacity(l.len());
                let mut num_removed = 0;

                for elem in l.drain(..).rev() {
                    if num_removed < -count && elem == value {
                        num_removed += 1;
                    } else {
                        new_list.push_front(elem);
                    }
                }

                *l = new_list;

                RespData::Integer(num_removed as i64)
            } else {
                let before_len = l.len();
                l.retain(|e| e != value);
                let after_len = l.len();

                RespData::Integer((before_len - after_len) as i64)
            }
        } else {
            Database::wrongtype()
        }
    }

    pub fn lset(&self, key: &str, index: isize, value: String) -> RespData {
        let bucket_ptr = {
            let map = self.map.read();

            if let Some(v) = map.get(key) {
                v.clone()
            } else {
                return Database::no_such_key();
            }
        };

        let mut bucket = bucket_ptr.write();

        if let Value::List(l) = &mut *bucket {
            let offset = if index < 0 {
                index + l.len() as isize
            } else {
                index
            };

            if offset < 0 || offset >= l.len() as isize {
                Database::out_of_range()
            } else {
                l[offset as usize] = value;

                Database::ok()
            }
        } else {
            Database::wrongtype()
        }
    }

    pub fn ltrim(&self, key: &str, start: isize, stop: isize) -> RespData {
        let map = self.map.upgradable_read();

        let bucket_ptr = if let Some(v) = map.get(key) {
            v.clone()
        } else {
            return Database::ok();
        };

        let mut bucket = bucket_ptr.write();

        if let Value::List(l) = &mut *bucket {
            let start_offset = if start < 0 {
                start + l.len() as isize
            } else {
                start
            };

            let stop_offset = if stop < 0 {
                stop + l.len() as isize
            } else {
                stop
            };

            let start_clamped = cmp::max(0, start_offset) as usize;
            let stop_clamped = cmp::min(l.len() as isize, stop_offset) as usize;

            if start_clamped >= l.len() || start_clamped > stop_clamped {
                let mut writer = RwLockUpgradableReadGuard::upgrade(map);

                writer.remove(key);
            } else {
                let numel = stop_clamped + 1 - start_clamped;

                l.drain(..start_clamped);
                l.drain(numel..);
            }

            Database::ok()
        } else {
            Database::wrongtype()
        }
    }

    pub fn rpop(&self, key: &str) -> RespData {
        let bucket_ptr = {
            let map = self.map.read();

            if let Some(b) = map.get(key) {
                b.clone()
            } else {
                return RespData::Nil;
            }
        };

        let mut bucket = bucket_ptr.write();

        if let Value::List(l) = &mut *bucket {
            if let Some(v) = l.pop_back() {
                RespData::BulkString(v)
            } else {
                RespData::Nil
            }
        } else {
            Database::wrongtype()
        }
    }

    pub fn rpush(&self, key: String, value: String) -> RespData {
        let bucket_ptr = {
            let map = self.map.upgradable_read();

            if let Some(v) = map.get(&key) {
                v.clone()
            } else {
                let mut writer = RwLockUpgradableReadGuard::upgrade(map);

                match writer.entry(key) {
                    Entry::Occupied(_) => unreachable!(), // should never happen, upgrade is atomic
                    Entry::Vacant(e) => {
                        let mut list = VecDeque::with_capacity(1);
                        list.push_back(value);

                        e.insert(Value::new(Value::List(list)));

                        return RespData::Integer(1);
                    }
                }
            }
        };

        let mut bucket = bucket_ptr.write();

        if let Value::List(list) = &mut *bucket {
            list.push_back(value);

            RespData::Integer(list.len() as i64)
        } else {
            Database::wrongtype()
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

    fn out_of_range() -> RespData {
        RespData::Error(
            "ERR index out of range".to_string(),
        )
    }

    fn no_such_key() -> RespData {
        RespData::Error(
            "ERR no such key".to_string(),
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
                    Entry::Occupied(_) => unreachable!(), // should never happen, upgrade is atomic
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
            _ => Database::wrongtype(),
        }
    }
}
