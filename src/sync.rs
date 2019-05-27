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

use std::{cell::UnsafeCell, ops::{Deref, DerefMut}, sync::Arc};

use tokio::prelude::*;
use parking_lot::{RawMutex, RawRwLock};
use lock_api::{RawMutex as MutexTrait, RawRwLock as RwLockTrait};

pub struct Mutex<T: ?Sized> {
    inner: Arc<InnerMutex<T>>,
}

impl<T> Mutex<T> {
    pub fn new(elem: T) -> Mutex<T> {
        Mutex{inner: Arc::new(InnerMutex{mutex: RawMutex::INIT, elem: UnsafeCell::new(elem)})}
    }
}

impl<T: ?Sized> Mutex<T> {
    pub fn lock(&self) -> MutexLock<T> {
        MutexLock{inner: self.inner.clone()}
    }
}

impl<T: ?Sized> Clone for Mutex<T> {
    fn clone(&self) -> Mutex<T> {
        Mutex{inner: self.inner.clone()}
    }
}

pub struct MutexLock<T: ?Sized> {
    inner: Arc<InnerMutex<T>>,
}

impl<T: ?Sized> Future for MutexLock<T> {
    type Item = MutexGuard<T>;
    type Error = ();

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        for _ in 0..40 {
            if self.inner.mutex.try_lock() {
                return Ok(Async::Ready(MutexGuard{inner: self.inner.clone()}))
            }
        }

        return Ok(Async::NotReady);
    }
}

pub struct MutexGuard<T: ?Sized> {
    inner: Arc<InnerMutex<T>>
}

impl<T: ?Sized> DerefMut for MutexGuard<T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.inner.elem.get() }
    }
}

impl<T: ?Sized> Deref for MutexGuard<T> {
    type Target = T;

    fn deref(&self) -> &T {
        unsafe { &*self.inner.elem.get() }
    }
}

impl<T: ?Sized> Drop for MutexGuard<T> {
    fn drop(&mut self) {
        self.inner.mutex.unlock();
    }
}

pub struct RwLock<T: ?Sized> {
    inner: Arc<InnerRwLock<T>>,
}

impl<T> RwLock<T> {
    pub fn new(elem: T) -> RwLock<T> {
        RwLock{inner: Arc::new(InnerRwLock{mutex: RawRwLock::INIT, elem: UnsafeCell::new(elem)})}
    }
}

impl<T: ?Sized> RwLock<T> {
    pub fn read(&self) -> RwLockRead<T> {
        RwLockRead{inner: self.inner.clone()}
    }

    pub fn write(&self) -> RwLockWrite<T> {
        RwLockWrite{inner: self.inner.clone()}
    }
}

impl<T: ?Sized> Clone for RwLock<T> {
    fn clone(&self) -> RwLock<T> {
        RwLock{inner: self.inner.clone()}
    }
}

pub struct RwLockRead<T: ?Sized> {
    inner: Arc<InnerRwLock<T>>,
}

impl<T: ?Sized> Future for RwLockRead<T> {
    type Item = RwLockReadGuard<T>;
    type Error = ();

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        for _ in 0..40 {
            if self.inner.mutex.try_lock_shared() {
                return Ok(Async::Ready(RwLockReadGuard{inner: self.inner.clone()}))
            }
        }

        return Ok(Async::NotReady);
    }
}

pub struct RwLockReadGuard<T: ?Sized> {
    inner: Arc<InnerRwLock<T>>
}

impl<T: ?Sized> Deref for RwLockReadGuard<T> {
    type Target = T;

    fn deref(&self) -> &T {
        unsafe { &*self.inner.elem.get() }
    }
}

impl<T: ?Sized> Drop for RwLockReadGuard<T> {
    fn drop(&mut self) {
        self.inner.mutex.unlock_shared();
    }
}

#[derive(Clone)]
pub struct RwLockWrite<T: ?Sized> {
    inner: Arc<InnerRwLock<T>>,
}

impl<T: ?Sized> Future for RwLockWrite<T> {
    type Item = RwLockWriteGuard<T>;
    type Error = ();

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        for _ in 0..40 {
            if self.inner.mutex.try_lock_exclusive() {
                return Ok(Async::Ready(RwLockWriteGuard{inner: self.inner.clone()}))
            }
        }

        return Ok(Async::NotReady);
    }
}

pub struct RwLockWriteGuard<T: ?Sized> {
    inner: Arc<InnerRwLock<T>>
}

impl<T: ?Sized> DerefMut for RwLockWriteGuard<T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.inner.elem.get() }
    }
}

impl<T: ?Sized> Deref for RwLockWriteGuard<T> {
    type Target = T;

    fn deref(&self) -> &T {
        unsafe { &*self.inner.elem.get() }
    }
}

impl<T: ?Sized> Drop for RwLockWriteGuard<T> {
    fn drop(&mut self) {
        self.inner.mutex.unlock_exclusive();
    }
}

struct InnerMutex<T: ?Sized> {
    mutex: RawMutex,
    elem: UnsafeCell<T>,
}

struct InnerRwLock<T: ?Sized> {
    mutex: RawRwLock,
    elem: UnsafeCell<T>,
}
