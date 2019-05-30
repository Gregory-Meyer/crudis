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

use std::{
    collections::VecDeque,
    hash::{BuildHasher, Hash, Hasher},
    mem,
    sync::atomic::{AtomicUsize, Ordering}
};

use crossbeam::epoch::{self, Atomic, Guard, Owned, Shared};
use fxhash::FxBuildHasher;

pub struct HashTable<H: BuildHasher> {
    buckets: Atomic<BucketArray>,
    hasher: H,
}

const REDIRECT_TAG: usize = 1;

impl HashTable<FxBuildHasher> {
    pub fn new() -> HashTable<FxBuildHasher> {
        HashTable::with_hasher(FxBuildHasher::default())
    }

    pub fn with_capacity(capacity: usize) -> HashTable<FxBuildHasher> {
        HashTable::with_capacity_and_hasher(capacity, FxBuildHasher::default())
    }
}

impl<H: BuildHasher> HashTable<H> {
    pub fn with_hasher(hasher: H) -> HashTable<H> {
        HashTable::with_capacity_and_hasher(8, hasher)
    }

    pub fn with_capacity_and_hasher(capacity: usize, hasher: H) -> HashTable<H> {
        HashTable{buckets: Atomic::new(BucketArray::with_capacity(8)), hasher}
    }

    // attempt to grow the hash table
    // 1. copy all buckets to the new table, exchange existing buckets with a
    // marker value that they should look at our new table instead
    // empty/deleted buckets should be marked as such as well
    // https://preshing.com/20160222/a-resizable-concurrent-map/
    // essentially CAS each bucket until we can set that bucket as redirect
    // and copy the new bucket into our bucket
    // however, reads should go right to the new hash table IMO
    // 2. CAS the BucketArrays, if that fails we can abort because someone else
    //    resized the table
    fn grow(&self) -> bool {
        let guard = &epoch::pin();
        let current_buckets_ptr = self.buckets.load(Ordering::SeqCst, guard);

        if current_buckets_ptr.is_null() {
            unimplemented!()
        }

        let current_buckets_ref = unsafe { current_buckets_ptr.deref() };

        if !current_buckets_ref.next_array.load(Ordering::SeqCst, guard).is_null() {
            return false;
        }

        let new_bucket_ptr = Owned::new(BucketArray::with_capacity(2 * current_buckets_ref.buckets.len())).into_shared(guard);

        if current_buckets_ref.next_array.compare_and_set(Shared::null(), new_bucket_ptr, Ordering::SeqCst, guard).is_err() {
            return false;
        }

        'outer: for i in 0..current_buckets_ref.buckets.len() {
            let this_bucket = &current_buckets_ref.buckets[i];
            let mut this_bucket_ptr = this_bucket.load(Ordering::SeqCst, guard);

            'inner: loop {
                if this_bucket_ptr.is_null() {
                    continue 'outer;
                }

                let this_bucket_ref = unsafe { this_bucket_ptr.deref() };

                unimplemented!()
            }
        }

        return self.buckets.compare_and_set(current_buckets_ptr, new_bucket_ptr, Ordering::SeqCst, guard).is_ok();
    }

    // insert a (key, value) pair or overwrite one that exists
    // return true if a matching key existed and was overwritten
    fn insert(&self, key: Vec<u8>, value: Value) -> bool {
        let guard = &epoch::pin();

        let mut bucket = Owned::new(Bucket::new(key, value));

        let hash = {
            let mut hasher = self.hasher.build_hasher();
            bucket.hash(&mut hasher);
            hasher.finish()
        };

        let mut buckets_ptr = self.buckets.load(Ordering::SeqCst, guard);

        loop {
            assert!(!buckets_ptr.is_null());

            let buckets_ref = unsafe { buckets_ptr.deref() };

            match buckets_ref.insert(bucket, hash, guard) {
                Ok(ptr) => {
                    if ptr.is_null() {
                        return false;
                    }

                    let removed_ref = unsafe { ptr.deref() };

                    return removed_ref.value.is_some();
                }
                Err(InsertError::Redirect(b)) => {
                    bucket = b;
                    buckets_ptr = buckets_ref.next_array.load(Ordering::SeqCst, guard);
                }
                Err(InsertError::Full(b)) => {
                    bucket = b;
                    self.grow();
                    buckets_ptr = self.buckets.load(Ordering::SeqCst, guard);
                }
            }
        }
    }

    // mutate a value or insert a default value
    // mutate_fn and default_fn may be called multiple times if there is
    // contention on that bucket
    // or also just because (spurious failure for example)
    fn mutate_or_insert<F: Fn(&Value) -> Value, G: Fn() -> Value>(&self, key: Vec<u8>, mutate_fn: F, default_fn: G) -> bool {
        unimplemented!()
    }

    // return a copy of a value in the table
    fn get(&self, key: &[u8]) -> Option<Value> {
        let guard = &epoch::pin();

        let hash = {
            let mut hasher = self.hasher.build_hasher();
            key.hash(&mut hasher);
            hasher.finish()
        };

        let mut buckets_ptr = self.buckets.load(Ordering::SeqCst, guard);

        loop {
            assert!(!buckets_ptr.is_null());

            let buckets_ref = unsafe { buckets_ptr.deref() };

            match buckets_ref.get(key, hash, guard) {
                Ok(found_bucket_ptr) => {
                    assert!(!found_bucket_ptr.is_null());

                    let found_bucket_ref = unsafe { found_bucket_ptr.deref() };
                    let value = found_bucket_ref.value.clone().unwrap();

                    return Some(value);
                }
                Err(FindError::Redirect) => {
                    buckets_ptr = buckets_ref.next_array.load(Ordering::SeqCst, guard);
                }
                Err(FindError::NotFound) => {
                    return None;
                }
            }
        }
    }

    // read a value, then use it
    fn get_and<T, F: FnOnce(&Value) -> T>(&self, key: &[u8], f: F) -> Option<T> {
        let guard = &epoch::pin();

        let hash = {
            let mut hasher = self.hasher.build_hasher();
            key.hash(&mut hasher);
            hasher.finish()
        };

        let mut buckets_ptr = self.buckets.load(Ordering::SeqCst, guard);

        loop {
            assert!(!buckets_ptr.is_null());

            let buckets_ref = unsafe { buckets_ptr.deref() };

            match buckets_ref.get(key, hash, guard) {
                Ok(found_bucket_ptr) => {
                    assert!(!found_bucket_ptr.is_null());

                    let found_bucket_ref = unsafe { found_bucket_ptr.deref() };

                    return Some(f(found_bucket_ref.value.as_ref().unwrap()));
                }
                Err(FindError::Redirect) => {
                    buckets_ptr = buckets_ref.next_array.load(Ordering::SeqCst, guard);
                }
                Err(FindError::NotFound) => {
                    return None;
                }
            }
        }
    }

    // remove the key matching a value
    fn remove(&self, key: Vec<u8>) -> bool {
        let guard = &epoch::pin();

        let hash = {
            let mut hasher = self.hasher.build_hasher();
            key.hash(&mut hasher);
            hasher.finish()
        };

        let mut buckets_ptr = self.buckets.load(Ordering::SeqCst, guard);

        loop {
            assert!(!buckets_ptr.is_null());

            let buckets_ref = unsafe { buckets_ptr.deref() };

            match buckets_ref.remove(key, hash, guard) {
                Ok(_) => return true,
                Err(RemoveError::Redirect(k)) => {
                    buckets_ptr = buckets_ref.next_array.load(Ordering::SeqCst, guard);
                    key = k;
                }
                Err(RemoveError::NotFound) => return false,
            }
        }
    }
}

struct BucketArray {
    buckets: Vec<Atomic<Bucket>>,
    len: AtomicUsize,
    next_array: Atomic<BucketArray>,
}

enum FindError {
    Redirect,
    NotFound,
}

enum InsertError {
    Redirect(Owned<Bucket>),
    Full(Owned<Bucket>),
}

enum RemoveError {
    Redirect(Vec<u8>),
    NotFound,
}

enum FindOrInsert<'g> {
    Found(Shared<'g, Bucket>),
    Inserted,
}

impl<'g> BucketArray {
    fn insert(&self, mut bucket: Owned<Bucket>, hash: u64, guard: &'g Guard) -> Result<Shared<'g, Bucket>, InsertError> {
        let len = self.buckets.len();
        let offset = (hash & (len - 1) as u64) as usize;

        let mut have_seen_redirect = true;

        for i in (0..self.buckets.len()).map(|x| (x + offset) & (len - 1)) {
            let this_bucket = &self.buckets[i];
            let mut this_bucket_ptr = this_bucket.load(Ordering::SeqCst, guard);

            loop {
                if this_bucket_ptr.tag() == REDIRECT_TAG {
                    have_seen_redirect = true;
                }

                if this_bucket_ptr.is_null() {
                    if this_bucket_ptr.tag() == REDIRECT_TAG {
                        return Err(InsertError::Redirect(bucket));
                    }

                    match this_bucket.compare_and_set_weak(this_bucket_ptr, bucket, Ordering::SeqCst, guard) {
                        Ok(_) => return Ok(this_bucket_ptr),
                        Err(e) => {
                            bucket = e.new;
                            this_bucket_ptr = e.current;
                        }
                    };
                } else {
                    let this_bucket_ref = unsafe { this_bucket_ptr.deref() };

                    if *this_bucket_ref == *bucket {
                        if this_bucket_ptr.tag() == REDIRECT_TAG {
                            return Err(InsertError::Redirect(bucket));
                        }

                        match this_bucket.compare_and_set_weak(this_bucket_ptr, bucket, Ordering::SeqCst, guard) {
                            Ok(_) => {
                                return Ok(this_bucket_ptr);
                            }
                            Err(e) => {
                                bucket = e.new;
                                this_bucket_ptr = e.current;
                            }
                        }
                    } else {
                        break;
                    }
                }
            }
        }

        if have_seen_redirect {
            Err(InsertError::Redirect(bucket))
        } else {
            Err(InsertError::Full(bucket))
        }
    }

    fn get(&self, key: &[u8], hash: u64, guard: &'g Guard) -> Result<Shared<'g, Bucket>, FindError> {
        let len = self.buckets.len();
        let offset = (hash & (len - 1) as u64) as usize;

        for i in (0..self.buckets.len()).map(|x| (x + offset) & (len - 1)) {
            let this_bucket = &self.buckets[i];
            let this_bucket_ptr = this_bucket.load(Ordering::SeqCst, guard);

            if this_bucket_ptr.is_null() {
                return Err(FindError::NotFound);
            } else {
                let this_bucket_ref = unsafe { this_bucket_ptr.deref() };

                if *this_bucket_ref == *key {
                    if this_bucket_ptr.tag() == REDIRECT_TAG {
                        return Err(FindError::Redirect);
                    }

                    match this_bucket_ref.value {
                        Some(_) => return Ok(this_bucket_ptr),
                        None => return Err(FindError::NotFound),
                    }
                }
            }
        }

        Err(FindError::NotFound)
    }

    fn remove(&self, key: Vec<u8>, hash: u64, guard: &'g Guard) -> Result<Shared<'g, Bucket>, RemoveError> {
        let len = self.buckets.len();
        let offset = (hash & (len - 1) as u64) as usize;

        let mut maybe_key = Some(key);
        let mut maybe_new_bucket: Option<Owned<Bucket>> = None;
        let mut key_ref = maybe_key.as_ref().unwrap();

        for i in (0..self.buckets.len()).map(|x| (x + offset) & (len - 1)) {
            let this_bucket = &self.buckets[i];
            let mut this_bucket_ptr = this_bucket.load(Ordering::SeqCst, guard);

            loop {
                if this_bucket_ptr.is_null() {
                    break;
                }

                let this_bucket_ref = unsafe { this_bucket_ptr.deref() };

                if *this_bucket_ref == *key_ref {
                    if this_bucket_ptr.tag() == REDIRECT_TAG {
                        match maybe_key {
                            Some(k) => return Err(RemoveError::Redirect(k)),
                            None => {
                                let new_bucket_ptr = maybe_new_bucket.unwrap().into_box();
                                let Bucket{key, value: _} = *new_bucket_ptr;

                                return Err(RemoveError::Redirect(key));
                            }
                        }
                    }

                    if this_bucket_ref.value.is_none() {
                        return Err(RemoveError::NotFound);
                    }

                    let new_bucket = match maybe_new_bucket.take() {
                        Some(b) => b,
                        None => Owned::new(Bucket::new_tombstone(maybe_key.take().unwrap())),
                    };

                    match this_bucket.compare_and_set_weak(this_bucket_ptr, new_bucket, Ordering::SeqCst, guard) {
                        Ok(_) => return Ok(this_bucket_ptr),
                        Err(e) => {
                            maybe_new_bucket.replace(e.new);
                            key_ref = &maybe_new_bucket.as_ref().unwrap().key;
                            this_bucket_ptr = e.current;
                        }
                    }
                } else {
                    break;
                }
            }
        }

        Err(RemoveError::NotFound)
    }
}

fn round_up_to_next_power_of_2(x: usize) -> usize {
    if is_power_of_2(x) {
        return x;
    }

    let first_set = (mem::size_of::<usize>() * 8) as u32 - x.leading_zeros();

    return 1 << first_set;
}

fn is_power_of_2(x: usize) -> bool {
    if x == 0 {
        false
    } else {
        (x & (x - 1)) == 0
    }
}


impl BucketArray {
    fn with_capacity(capacity: usize) -> BucketArray {
        BucketArray{buckets: vec![Atomic::null(); capacity], len: AtomicUsize::new(0), next_array: Atomic::null()}
    }

    fn capacity(&self) -> usize {
        self.buckets.len()
    }
}

struct Bucket {
    key: Vec<u8>,
    value: Option<Value>,
}

impl Bucket {
    fn new(key: Vec<u8>, value: Value) -> Bucket {
        Bucket{key, value: Some(value)}
    }

    fn new_tombstone(key: Vec<u8>) -> Bucket {
        Bucket{key, value: None}
    }
}

impl Eq for Bucket { }

impl PartialEq for Bucket {
    fn eq(&self, other: &Bucket) -> bool {
        self.key == other.key
    }
}

impl PartialEq<Vec<u8>> for Bucket {
    fn eq(&self, other: &Vec<u8>) -> bool {
        self.key == *other
    }
}

impl PartialEq<[u8]> for Bucket {
    fn eq(&self, other: &[u8]) -> bool {
        self.key == other
    }
}

impl Hash for Bucket {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.key.hash(state);
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum Value {
    String(Vec<u8>),
    List(VecDeque<String>), //TODO: use im::Vector here
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert() {
        let table = HashTable::new();

        assert!(!table.insert(b"foo".to_vec(), Value::String(b"foo".to_vec())));
        assert!(table.insert(b"foo".to_vec(), Value::String(b"foo".to_vec())));

        assert!(!table.insert(b"bar".to_vec(), Value::String(b"bar".to_vec())));
        assert!(table.insert(b"foo".to_vec(), Value::String(b"foo".to_vec())));
        assert!(table.insert(b"bar".to_vec(), Value::String(b"bar".to_vec())));

        assert!(!table.insert(b"baz".to_vec(), Value::String(b"baz".to_vec())));
        assert!(table.insert(b"foo".to_vec(), Value::String(b"foo".to_vec())));
        assert!(table.insert(b"bar".to_vec(), Value::String(b"bar".to_vec())));
        assert!(table.insert(b"baz".to_vec(), Value::String(b"baz".to_vec())));

        assert!(!table.insert(b"qux".to_vec(), Value::String(b"qux".to_vec())));
        assert!(table.insert(b"foo".to_vec(), Value::String(b"foo".to_vec())));
        assert!(table.insert(b"bar".to_vec(), Value::String(b"bar".to_vec())));
        assert!(table.insert(b"baz".to_vec(), Value::String(b"baz".to_vec())));
        assert!(table.insert(b"qux".to_vec(), Value::String(b"qux".to_vec())));
    }

    #[test]
    fn get() {
        let table = HashTable::new();

        assert!(table.get(b"foo").is_none());
        assert!(table.get(b"bar").is_none());
        assert!(table.get(b"baz").is_none());
        assert!(table.get(b"qux").is_none());

        assert!(!table.insert(b"foo".to_vec(), Value::String(b"foo".to_vec())));
        assert_eq!(table.get(b"foo"), Some(Value::String(b"foo".to_vec())));
        assert!(table.get(b"bar").is_none());
        assert!(table.get(b"baz").is_none());
        assert!(table.get(b"qux").is_none());

        assert!(!table.insert(b"bar".to_vec(), Value::String(b"bar".to_vec())));
        assert_eq!(table.get(b"foo"), Some(Value::String(b"foo".to_vec())));
        assert_eq!(table.get(b"bar"), Some(Value::String(b"bar".to_vec())));
        assert!(table.get(b"baz").is_none());
        assert!(table.get(b"qux").is_none());

        assert!(!table.insert(b"baz".to_vec(), Value::String(b"baz".to_vec())));
        assert_eq!(table.get(b"foo"), Some(Value::String(b"foo".to_vec())));
        assert_eq!(table.get(b"bar"), Some(Value::String(b"bar".to_vec())));
        assert_eq!(table.get(b"baz"), Some(Value::String(b"baz".to_vec())));
        assert!(table.get(b"qux").is_none());

        assert!(!table.insert(b"qux".to_vec(), Value::String(b"qux".to_vec())));
        assert_eq!(table.get(b"foo"), Some(Value::String(b"foo".to_vec())));
        assert_eq!(table.get(b"bar"), Some(Value::String(b"bar".to_vec())));
        assert_eq!(table.get(b"baz"), Some(Value::String(b"baz".to_vec())));
        assert_eq!(table.get(b"qux"), Some(Value::String(b"qux".to_vec())));
    }
}
