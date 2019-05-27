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

use crate::resp::{BulkStringRef, ErrorRef, RespData, SimpleStringRef};

use std::{cmp, collections::VecDeque, io, mem, sync::Arc};

use hashbrown::{hash_map::Entry, HashMap, HashSet};
use lock_api::RwLockUpgradableReadGuard;
use parking_lot::RwLock;

pub enum Value {
    String(String),
    List(VecDeque<String>),
    Set(HashSet<String>),
    Hash(HashMap<String, String>),
}

type Bucket = (Value, Option<()>);

impl Value {
    fn new(value: Value) -> Arc<RwLock<Bucket>> {
        Arc::new(RwLock::new((value, None)))
    }
}

#[derive(Clone)]
pub struct Database {
    map: Arc<RwLock<HashMap<String, Arc<RwLock<Bucket>>>>>,
}

impl Database {
    pub fn new() -> Database {
        Database {
            map: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn decr<W: io::Write>(&self, key: String, writer: &mut W) -> io::Result<()> {
        self.decrby(key, 1, writer)
    }

    pub fn decrby<W: io::Write>(
        &self,
        key: String,
        decrement: i64,
        writer: &mut W,
    ) -> io::Result<()> {
        self.rmw_integer(key, |x| x - decrement, || -decrement, writer)
    }

    pub fn get<W: io::Write>(&self, key: &str, writer: &mut W) -> io::Result<()> {
        let bucket_ptr = {
            let map = self.map.read();

            if let Some(v) = map.get(key) {
                v.clone()
            } else {
                return write!(writer, "{}", RespData::Nil);
            }
        };

        let bucket = bucket_ptr.read();

        if let Value::String(value) = &bucket.0 {
            write!(writer, "{}", BulkStringRef(value))
        } else {
            write!(writer, "{}", Database::wrongtype())
        }
    }

    pub fn getset<W: io::Write>(
        &self,
        key: String,
        mut value: String,
        writer: &mut W,
    ) -> io::Result<()> {
        let bucket_ptr = {
            let map = self.map.upgradable_read();

            if let Some(v) = map.get(&key) {
                v.clone()
            } else {
                let mut map = RwLockUpgradableReadGuard::upgrade(map);

                match map.entry(key) {
                    Entry::Occupied(_) => unreachable!(), // this should never happen
                    Entry::Vacant(e) => {
                        e.insert(Value::new(Value::String(value)));

                        return write!(writer, "{}", RespData::Nil);
                    }
                }
            }
        };

        let mut bucket = bucket_ptr.write();

        match &mut bucket.0 {
            Value::String(s) => {
                mem::swap(s, &mut value);

                write!(writer, "{}", BulkStringRef(&value))
            }
            _ => write!(writer, "{}", Database::wrongtype()),
        }
    }

    pub fn incr<W: io::Write>(&self, key: String, writer: &mut W) -> io::Result<()> {
        self.incrby(key, 1, writer)
    }

    pub fn incrby<W: io::Write>(
        &self,
        key: String,
        increment: i64,
        writer: &mut W,
    ) -> io::Result<()> {
        self.rmw_integer(key, |x| x + increment, || increment, writer)
    }

    pub fn mget<S: AsRef<str>, W: io::Write>(&self, keys: &[S], writer: &mut W) -> io::Result<()> {
        let maybe_bucket_ptrs: Vec<_> = {
            let map = self.map.read();

            keys.iter().map(|k| map.get(k.as_ref()).cloned()).collect()
        };

        write!(writer, "*{}\r\n", maybe_bucket_ptrs.len())?;

        for maybe_ptr in maybe_bucket_ptrs.into_iter() {
            if let Some(ptr) = maybe_ptr {
                let elem = ptr.read();

                if let Value::String(s) = &elem.0 {
                    write!(writer, "{}", BulkStringRef(&s))?;
                } else {
                    write!(writer, "{}", RespData::Nil)?;
                }
            } else {
                write!(writer, "{}", RespData::Nil)?;
            }
        }

        Ok(())
    }

    pub fn set<W: io::Write>(&self, key: String, value: String, writer: &mut W) -> io::Result<()> {
        let bucket_ptr = {
            let map = self.map.upgradable_read();

            if let Some(v) = map.get(&key) {
                v.clone()
            } else {
                let mut map = RwLockUpgradableReadGuard::upgrade(map);

                match map.entry(key) {
                    Entry::Occupied(_) => unreachable!(), // should never happen, upgrade is atomic
                    Entry::Vacant(e) => {
                        e.insert(Value::new(Value::String(value)));

                        return write!(writer, "{}", Database::ok());
                    }
                }
            }
        };

        let mut bucket = bucket_ptr.write();

        match &mut bucket.0 {
            Value::String(s) => *s = value,
            _ => bucket.0 = Value::String(value),
        }

        write!(writer, "{}", Database::ok())
    }

    pub fn setnx<W: io::Write>(
        &self,
        key: String,
        value: String,
        writer: &mut W,
    ) -> io::Result<()> {
        let map = self.map.upgradable_read();

        if let Some(_) = map.get(&key) {
            return write!(writer, "{}", RespData::Integer(0));
        }

        let mut map = RwLockUpgradableReadGuard::upgrade(map);

        match map.entry(key) {
            Entry::Occupied(_) => unreachable!(), // should never happen, upgrade is atomic
            Entry::Vacant(e) => {
                e.insert(Value::new(Value::String(value)));

                write!(writer, "{}", RespData::Integer(1))
            }
        }
    }

    pub fn lindex<W: io::Write>(&self, key: &str, index: isize, writer: &mut W) -> io::Result<()> {
        let bucket_ptr = {
            let map = self.map.read();

            if let Some(b) = map.get(key) {
                b.clone()
            } else {
                return write!(writer, "{}", RespData::Nil);
            }
        };

        let bucket = bucket_ptr.read();

        if let Value::List(l) = &bucket.0 {
            let offset = if index < 0 {
                index + l.len() as isize
            } else {
                index
            };

            if offset < 0 || offset as usize >= l.len() {
                write!(writer, "{}", RespData::Nil)
            } else {
                write!(writer, "{}", BulkStringRef(&l[offset as usize]))
            }
        } else {
            write!(writer, "{}", Database::wrongtype())
        }
    }

    pub fn llen<W: io::Write>(&self, key: &str, writer: &mut W) -> io::Result<()> {
        let bucket_ptr = {
            let map = self.map.read();

            if let Some(b) = map.get(key) {
                b.clone()
            } else {
                return write!(writer, "{}", RespData::Integer(0));
            }
        };

        let bucket = bucket_ptr.read();

        if let Value::List(l) = &bucket.0 {
            write!(writer, "{}", RespData::Integer(l.len() as i64))
        } else {
            write!(writer, "{}", Database::wrongtype())
        }
    }

    pub fn lpop<W: io::Write>(&self, key: &str, writer: &mut W) -> io::Result<()> {
        let bucket_ptr = {
            let map = self.map.read();

            if let Some(b) = map.get(key) {
                b.clone()
            } else {
                return write!(writer, "{}", RespData::Nil);
            }
        };

        let mut bucket = bucket_ptr.write();

        if let Value::List(l) = &mut bucket.0 {
            if let Some(v) = l.pop_front() {
                write!(writer, "{}", BulkStringRef(&v))
            } else {
                write!(writer, "{}", RespData::Nil)
            }
        } else {
            write!(writer, "{}", Database::wrongtype())
        }
    }

    pub fn lpush<W: io::Write>(
        &self,
        key: String,
        value: String,
        writer: &mut W,
    ) -> io::Result<()> {
        let bucket_ptr = {
            let map = self.map.upgradable_read();

            if let Some(v) = map.get(&key) {
                v.clone()
            } else {
                let mut map = RwLockUpgradableReadGuard::upgrade(map);

                match map.entry(key) {
                    Entry::Occupied(_) => unreachable!(), // should never happen, upgrade is atomic
                    Entry::Vacant(e) => {
                        let mut list = VecDeque::with_capacity(1);
                        list.push_front(value);

                        e.insert(Value::new(Value::List(list)));

                        return write!(writer, "{}", RespData::Integer(1));
                    }
                }
            }
        };

        let mut bucket = bucket_ptr.write();

        if let Value::List(list) = &mut bucket.0 {
            list.push_front(value);

            return write!(writer, "{}", RespData::Integer(list.len() as i64));
        } else {
            return write!(writer, "{}", Database::wrongtype());
        }
    }

    pub fn lrange<W: io::Write>(
        &self,
        key: &str,
        start: isize,
        stop: isize,
        writer: &mut W,
    ) -> io::Result<()> {
        let bucket_ptr = {
            let map = self.map.read();

            if let Some(v) = map.get(key) {
                v.clone()
            } else {
                return writer.write_all(b"*0\r\n");
            }
        };

        let bucket = bucket_ptr.read();

        if let Value::List(l) = &bucket.0 {
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
                writer.write_all(b"*0\r\n")
            } else {
                let numel = stop_clamped + 1 - start_clamped;

                write!(writer, "*{}\r\n", numel)?;

                for elem in l.iter().skip(start_clamped).take(numel) {
                    write!(writer, "{}", BulkStringRef(elem))?;
                }

                Ok(())
            }
        } else {
            write!(writer, "{}", Database::wrongtype())
        }
    }

    pub fn lrem<W: io::Write>(
        &self,
        key: &str,
        count: isize,
        value: &str,
        writer: &mut W,
    ) -> io::Result<()> {
        let bucket_ptr = {
            let map = self.map.read();

            if let Some(v) = map.get(key) {
                v.clone()
            } else {
                return write!(writer, "{}", RespData::Integer(0));
            }
        };

        let mut bucket = bucket_ptr.write();

        if let Value::List(l) = &mut bucket.0 {
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

                write!(writer, "{}", RespData::Integer(num_removed as i64))
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

                write!(writer, "{}", RespData::Integer(num_removed as i64))
            } else {
                let before_len = l.len();
                l.retain(|e| e != value);
                let after_len = l.len();

                write!(
                    writer,
                    "{}",
                    RespData::Integer((before_len - after_len) as i64)
                )
            }
        } else {
            write!(writer, "{}", Database::wrongtype())
        }
    }

    pub fn lset<W: io::Write>(
        &self,
        key: &str,
        index: isize,
        value: String,
        writer: &mut W,
    ) -> io::Result<()> {
        let bucket_ptr = {
            let map = self.map.read();

            if let Some(v) = map.get(key) {
                v.clone()
            } else {
                return write!(writer, "{}", Database::no_such_key());
            }
        };

        let mut bucket = bucket_ptr.write();

        if let Value::List(l) = &mut bucket.0 {
            let offset = if index < 0 {
                index + l.len() as isize
            } else {
                index
            };

            if offset < 0 || offset >= l.len() as isize {
                write!(writer, "{}", Database::out_of_range())
            } else {
                l[offset as usize] = value;

                write!(writer, "{}", Database::ok())
            }
        } else {
            write!(writer, "{}", Database::wrongtype())
        }
    }

    pub fn ltrim<W: io::Write>(
        &self,
        key: &str,
        start: isize,
        stop: isize,
        writer: &mut W,
    ) -> io::Result<()> {
        let map = self.map.upgradable_read();

        let bucket_ptr = if let Some(v) = map.get(key) {
            v.clone()
        } else {
            return write!(writer, "{}", Database::ok());
        };

        let mut bucket = bucket_ptr.write();

        if let Value::List(l) = &mut bucket.0 {
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

            write!(writer, "{}", Database::ok())
        } else {
            write!(writer, "{}", Database::wrongtype())
        }
    }

    pub fn rpop<W: io::Write>(&self, key: &str, writer: &mut W) -> io::Result<()> {
        let bucket_ptr = {
            let map = self.map.read();

            if let Some(b) = map.get(key) {
                b.clone()
            } else {
                return write!(writer, "{}", RespData::Nil);
            }
        };

        let mut bucket = bucket_ptr.write();

        if let Value::List(l) = &mut bucket.0 {
            if let Some(v) = l.pop_back() {
                write!(writer, "{}", BulkStringRef(&v))
            } else {
                write!(writer, "{}", RespData::Nil)
            }
        } else {
            write!(writer, "{}", Database::wrongtype())
        }
    }

    pub fn rpush<W: io::Write>(
        &self,
        key: String,
        value: String,
        writer: &mut W,
    ) -> io::Result<()> {
        let bucket_ptr = {
            let map = self.map.upgradable_read();

            if let Some(v) = map.get(&key) {
                v.clone()
            } else {
                let mut map = RwLockUpgradableReadGuard::upgrade(map);

                match map.entry(key) {
                    Entry::Occupied(_) => unreachable!(), // should never happen, upgrade is atomic
                    Entry::Vacant(e) => {
                        let mut list = VecDeque::with_capacity(1);
                        list.push_back(value);

                        e.insert(Value::new(Value::List(list)));

                        return write!(writer, "{}", RespData::Integer(1));
                    }
                }
            }
        };

        let mut bucket = bucket_ptr.write();

        if let Value::List(list) = &mut bucket.0 {
            list.push_back(value);

            write!(writer, "{}", RespData::Integer(list.len() as i64))
        } else {
            write!(writer, "{}", Database::wrongtype())
        }
    }

    pub fn del<S: AsRef<str>, W: io::Write>(&self, keys: &[S], writer: &mut W) -> io::Result<()> {
        let mut map = self.map.write();

        let num_removed = keys
            .iter()
            .map(|k| map.remove(k.as_ref()).is_some())
            .fold(0, |p, n| p + n as i64);

        write!(writer, "{}", RespData::Integer(num_removed))
    }

    pub fn exists<W: io::Write>(&self, key: &str, writer: &mut W) -> io::Result<()> {
        let map = self.map.read();

        write!(
            writer,
            "{}",
            RespData::Integer(map.contains_key(key) as i64)
        )
    }

    fn ok() -> SimpleStringRef<'static> {
        SimpleStringRef("OK")
    }

    fn wrongtype() -> ErrorRef<'static> {
        ErrorRef("WRONGTYPE Operation against a key holding the wrong kind of value")
    }

    fn out_of_range() -> ErrorRef<'static> {
        ErrorRef("ERR index out of range")
    }

    fn no_such_key() -> ErrorRef<'static> {
        ErrorRef("ERR no such key")
    }

    fn rmw_integer<W: io::Write, F: FnOnce(i64) -> i64, G: FnOnce() -> i64>(
        &self,
        key: String,
        if_present: F,
        if_absent: G,
        writer: &mut W,
    ) -> io::Result<()> {
        let bucket_ptr = {
            let map = self.map.upgradable_read();

            if let Some(v) = map.get(&key) {
                v.clone()
            } else {
                let mut map_writer = RwLockUpgradableReadGuard::upgrade(map);

                match map_writer.entry(key) {
                    Entry::Occupied(_) => unreachable!(), // should never happen, upgrade is atomic
                    Entry::Vacant(e) => {
                        let val = if_absent();
                        e.insert(Value::new(Value::String(format!("{}", val))));

                        return write!(writer, "{}", RespData::Integer(val));
                    }
                }
            }
        };

        let mut bucket = bucket_ptr.write();

        match &mut bucket.0 {
            Value::String(s) => {
                if let Ok(i) = s.parse::<i64>().map(if_present) {
                    *s = format!("{}", i);

                    write!(writer, "{}", RespData::Integer(i))
                } else {
                    write!(
                        writer,
                        "{}",
                        ErrorRef("ERR value is not an integer or out of range")
                    )
                }
            }
            _ => write!(writer, "{}", Database::wrongtype()),
        }
    }
}
