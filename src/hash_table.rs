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

use std::{collections::VecDeque, sync::atomic::{AtomicUsize, Ordering}};

use crossbeam::epoch::{self, Atomic, Guard, Owned, Shared};

pub struct HashTable {
    buckets: Atomic<BucketArray>,
}

const REDIRECT_TAG: usize = 1;
const TOMBSTONE_TAG: usize = 2;

impl HashTable {
    pub fn new() -> HashTable {
        HashTable{buckets: Atomic::new(BucketArray::with_capacity(8))}
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

        let buckets = self.buckets.load(Ordering::SeqCst, guard);
        assert!(!buckets.is_null());

        let buckets_ref = unsafe { buckets.deref() };
        let new_buckets = Owned::new(BucketArray::with_capacity(buckets_ref.capacity() * 2)).into_shared(guard);
        let new_buckets_ref = unsafe { new_buckets.deref() };

        if !buckets_ref.next_array.compare_and_set(Shared::null(), new_buckets, Ordering::SeqCst, guard).is_ok() {
            return false;
        }

        let null_redirect = Shared::null().with_tag(REDIRECT_TAG);

        for bucket in buckets_ref.buckets.iter() {
            let mut this_bucket = bucket.load(Ordering::SeqCst, guard);

            loop {
                if this_bucket.is_null() && this_bucket.tag() == REDIRECT_TAG {
                    return false;
                } else if this_bucket.is_null() {
                    if let Err(e) = bucket.compare_and_set_weak(this_bucket, null_redirect, Ordering::SeqCst, guard) {
                        this_bucket = e.current;

                        continue;
                    } else {
                        break;
                    }
                }

                let this_bucket_ref = unsafe { this_bucket.deref() };
                let hash = fxhash::hash(&this_bucket_ref.key);
                let insert_idx = new_buckets_ref.find_for_insert(hash, guard).unwrap();

                new_buckets_ref.buckets[insert_idx].store(this_bucket, Ordering::SeqCst);

                if let Err(e) = bucket.compare_and_set_weak(this_bucket, null_redirect, Ordering::SeqCst, guard) {
                    this_bucket = e.current;
                } else {
                    break;
                }
            }

        }

        self.buckets.compare_and_set(buckets, new_buckets, Ordering::SeqCst, guard).is_ok()
    }

    // insert a (key, value) pair or overwrite one that exists
    // return true if the pair existed and was removed
    fn insert_or_assign(&self, key: &[u8], value: Value) -> bool {
        let guard = &epoch::pin();

        let hash = fxhash::hash(&key);
        let mut buckets = self.buckets.load(Ordering::SeqCst, guard);

        loop {
            let buckets_ref = unsafe { buckets.deref() };

            match buckets_ref.find_or_insert(key, hash, || value.clone(), guard) {
                Ok((bucket, inserted)) => {
                    if inserted {
                        return true;
                    }

                    let bucket_ref = unsafe { bucket.deref() };
                    bucket_ref.value.store(Owned::new(value), Ordering::SeqCst);

                    return false;
                }
                Err(FindError::Redirect) => {
                    buckets = buckets_ref.next_array.load(Ordering::SeqCst, guard);
                }
                Err(FindError::Full) => {
                    self.grow();
                },
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
    fn get(&self, key: &[u8]) -> Value {
        unimplemented!()
    }

    // read a value, then use it
    fn get_and<F: FnOnce(&Value)>(&self, key: &[u8], f: F) {
        unimplemented!()
    }
}

struct BucketArray {
    buckets: Vec<Atomic<Bucket>>,
    len: AtomicUsize,
    next_array: Atomic<BucketArray>,
}

enum FindError {
    Redirect,
    Full
}

impl<'a> BucketArray {
    fn find_or_insert<F: Fn() -> Value>(&self, key: &[u8], hash: usize, default_fn: F, guard: &'a Guard) -> Result<(Shared<'a, Bucket>, bool), FindError> {
        let mut bucket_index = hash & (self.buckets.len() - 1);

        let mut count = 0;
        let mut maybe_new_bucket = None;

        while count < self.buckets.len() {
            let this_bucket = self.buckets[bucket_index].load(Ordering::SeqCst, guard);

            if this_bucket.is_null() {
                if this_bucket.tag() == 0 {
                    if maybe_new_bucket.is_none() {
                        maybe_new_bucket.replace(Owned::new(Bucket{
                            key: key.to_vec(),
                            value: Atomic::new(default_fn())
                        }));
                    }

                    match self.buckets[bucket_index].compare_and_set(
                        this_bucket,
                        maybe_new_bucket.take().unwrap(),
                        Ordering::SeqCst,
                        guard
                    ) {
                        Ok(b) => return Ok((b, true)),
                        Err(e) => {
                            maybe_new_bucket.replace(e.new);

                            continue;
                        }
                    }
                } else if this_bucket.tag() == REDIRECT_TAG {
                    return Err(FindError::Redirect)
                } else if this_bucket.tag() == TOMBSTONE_TAG { // keep looking
                    count += 1;
                    bucket_index += 1;
                    if bucket_index == self.buckets.len() {
                        bucket_index = 0;
                    }

                    continue;
                }
            }

            let this_bucket_ref = unsafe { this_bucket.deref() };

            if this_bucket_ref.key == key {
                return Ok((this_bucket, false))
            }

            count += 1;
            bucket_index += 1;
            if bucket_index == self.buckets.len() {
                bucket_index = 0;
            }
        }

        Err(FindError::Full)
    }
}

impl BucketArray {
    fn with_capacity(capacity: usize) -> BucketArray {
        BucketArray{buckets: vec![Atomic::null(); capacity], len: AtomicUsize::new(0), next_array: Atomic::null()}
    }

    fn capacity(&self) -> usize {
        self.buckets.len()
    }

    fn find_for_insert(&self, hash: usize, guard: &Guard) -> Option<usize> {
        let mut bucket_index = hash & (self.buckets.len() - 1);

        for _ in 0..self.buckets.len() {
            if self.buckets[bucket_index].load(Ordering::SeqCst, guard).is_null() {
                return Some(bucket_index);
            }

            bucket_index += 1;
            if bucket_index == self.buckets.len() {
                bucket_index = 0;
            }
        }

        None
    }
}

struct Bucket {
    key: Vec<u8>,
    value: Atomic<Value>,
}

#[derive(Clone)]
enum Value {
    String(Vec<u8>),
    List(VecDeque<String>),
}
