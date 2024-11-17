use crate::backoff::Backoff;
use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering::*};
use crossbeam::utils::CachePadded;
use std::sync::atomic::AtomicU32;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct SpinLimit(pub u32);
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct YieldLimit(pub u32);

impl Default for SpinLimit {
    #[inline]
    fn default() -> Self {
        Self(6)
    }
}
impl Default for YieldLimit {
    #[inline]
    fn default() -> Self {
        Self(10)
    }
}

#[derive(Debug)]
struct AtomicCounter {
    counter: CachePadded<AtomicUsize>,
    max: usize,
    sublevel: Vec<AtomicCounter>,
}

impl AtomicCounter {
    fn new(nthreads: usize) -> Self {
        for k in [4, 3, 2] {
            if nthreads % k == 0 {
                return Self {
                    counter: CachePadded::new(AtomicUsize::new(k)),
                    max: k,
                    sublevel: (0..k).map(|_| Self::new(nthreads / k)).collect(),
                };
            }
        }

        Self {
            counter: CachePadded::new(AtomicUsize::new(nthreads)),
            max: nthreads,
            sublevel: vec![],
        }
    }

    fn dec(&self, tid: usize, nthreads: usize) -> bool {
        let k = self.sublevel.len();

        if k == 0 || self.sublevel[(tid * k) / nthreads].dec(tid % (nthreads / k), nthreads / k) {
            return self.counter.fetch_sub(1, AcqRel) == 1;
        }
        false
    }

    fn reset(&self) {
        self.counter.store(self.max, Release);
        for c in &self.sublevel {
            c.reset();
        }
    }
}

#[derive(Debug)]
pub struct BarrierInit {
    done: AtomicBool,
    waiting_for_leader: CachePadded<AtomicU32>,
    gsense: CachePadded<AtomicU32>,
    count: AtomicCounter,
    max: usize,

    spin_limit: SpinLimit,
    yield_limit: YieldLimit,
}

#[derive(Debug)]
pub struct BarrierRef<'a> {
    init: &'a BarrierInit,
    lsense: bool,
    tid: usize,
}
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum BarrierWaitResult {
    Leader,
    Follower,
    Dropped,
}

impl BarrierInit {
    #[inline]
    pub fn autotune(num_threads: usize) -> (SpinLimit, YieldLimit) {
        crate::backoff::best_limit_bench(num_threads)
    }

    #[inline]
    pub fn new(num_threads: usize, spin_limit: SpinLimit, yield_limit: YieldLimit) -> Self {
        Self {
            done: AtomicBool::new(false),
            waiting_for_leader: CachePadded::new(AtomicU32::new(false as u32)),
            count: AtomicCounter::new(num_threads),
            gsense: CachePadded::new(AtomicU32::new(false as u32)),
            max: num_threads,

            spin_limit,
            yield_limit,
        }
    }

    #[inline]
    pub fn num_threads(&self) -> usize {
        self.max
    }

    pub fn barrier_ref(&self, tid: usize) -> BarrierRef<'_> {
        BarrierRef {
            init: self,
            lsense: false,
            tid,
        }
    }
}

impl BarrierRef<'_> {
    #[inline]
    pub fn num_threads(&self) -> usize {
        self.init.max
    }

    #[inline(never)]
    pub fn wait(&mut self) -> BarrierWaitResult {
        self.lsense = !self.lsense;

        if self.init.count.dec(self.tid, self.init.max) {
            self.init.waiting_for_leader.store(true as u32, Release);
            self.init.count.reset();
            self.init.gsense.store(self.lsense as u32, Release);

            atomic_wait::wake_all(&*self.init.gsense);

            BarrierWaitResult::Leader
        } else {
            let wait = Backoff::new(
                self.init.spin_limit.0,
                self.init.yield_limit.0,
                self.init.max as u32,
            );

            loop {
                let done = self.init.done.load(Acquire);

                if (self.init.gsense.load(Acquire) != 0) == self.lsense {
                    break;
                }

                if done {
                    return BarrierWaitResult::Dropped;
                }

                if wait.is_completed() {
                    atomic_wait::wait(&*self.init.gsense, (!self.lsense) as u32);
                } else {
                    wait.snooze();
                }
            }
            BarrierWaitResult::Follower
        }
    }

    #[inline]
    pub fn lead(&self) {
        self.init.waiting_for_leader.store(false as u32, Release);
        atomic_wait::wake_all(&*self.init.waiting_for_leader);
    }

    #[inline(never)]
    pub fn follow(&self) {
        let wait = Backoff::new(
            self.init.spin_limit.0,
            self.init.yield_limit.0,
            self.init.max as u32,
        );

        loop {
            if self.init.waiting_for_leader.load(Acquire) == 0 {
                break;
            }

            if wait.is_completed() {
                atomic_wait::wait(&self.init.waiting_for_leader, true as u32);
            } else {
                wait.snooze();
            }
        }
    }
}
