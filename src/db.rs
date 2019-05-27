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

use crate::sync::{RwLock, RwLockRead, RwLockWrite};

use std::collections::VecDeque;

use hashbrown::{HashMap, hash_map::Entry};
use tokio::prelude::*;

type DatabaseMap = HashMap<Vec<u8>, RwLock<Value>>;
type DatabaseInner = RwLock<DatabaseMap>;

pub struct Database {
    inner: DatabaseInner,
}

impl Database {
    fn bucket(&self, key: Vec<u8>) -> Bucket {
        Bucket{read: self.inner.read(), key}
    }

    fn bucket_or_else<F: FnOnce() -> Value>(&self, key: Vec<u8>, or_else: F) -> BucketOrElse<F> {
        BucketOrElse{write: self.inner.write(), key: Some(key), or_else: Some(or_else)}
    }
}

struct Bucket {
    read: RwLockRead<DatabaseMap>,
    key: Vec<u8>,
}

impl Future for Bucket {
    type Item = RwLock<Value>;
    type Error = NoSuchBucketError;

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        if let Ok(Async::Ready(guard)) = self.read.poll() {
            if let Some(value) = guard.get(&self.key) {
                Ok(Async::Ready(value.clone()))
            } else {
                Err(NoSuchBucketError{})
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

struct NoSuchBucketError {

}
