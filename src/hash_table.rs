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
}

impl<H: BuildHasher> HashTable<H> {
    pub fn with_hasher(hasher: H) -> HashTable<H> {
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
        unimplemented!()
    }

    // insert a (key, value) pair or overwrite one that exists
    // return true if the pair existed and was removed
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
                Ok(ptr) => return ptr.is_null(),
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
}

struct BucketArray {
    buckets: Vec<Atomic<Bucket>>,
    len: AtomicUsize,
    next_array: Atomic<BucketArray>,
}

enum FindError {
    Redirect,
    NotFound
}

enum InsertError {
    Redirect(Owned<Bucket>),
    Full(Owned<Bucket>),
}

enum FindOrInsert<'g> {
    Found(Shared<'g, Bucket>),
    Inserted,
}

impl<'g> BucketArray {
    fn find_or_insert<F: Fn() -> Value>(&self, key: Vec<u8>, hash: u64, default_fn: F, guard: &'g Guard) -> Result<FindOrInsert<'g>, FindError> {
        unimplemented!()
    }

    fn insert(&self, mut bucket: Owned<Bucket>, hash: u64, guard: &'g Guard) -> Result<Shared<'g, Bucket>, InsertError> {
        let len = self.buckets.len();
        let offset = (hash & (len - 1) as u64) as usize;

        for i in (0..self.buckets.len()).map(|x| (x + offset) & (len - 1)) {
            let this_bucket = &self.buckets[i];
            let mut this_bucket_ptr = this_bucket.load(Ordering::SeqCst, guard);

            loop {
                if this_bucket_ptr.is_null() {
                    match this_bucket.compare_and_set_weak(this_bucket_ptr, bucket, Ordering::SeqCst, guard) {
                        Ok(prev) => return Ok(prev),
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
                            Ok(prev) => return Ok(prev),
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

        Err(InsertError::Full(bucket))
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

#[derive(Clone)]
enum Value {
    String(Vec<u8>),
    List(VecDeque<String>), //TODO: use im::Vector here
}
