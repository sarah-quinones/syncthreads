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
pub struct BarrierInit {
    done: AtomicBool,
    waiting_for_leader: CachePadded<AtomicU32>,
    gsense: CachePadded<AtomicU32>,

    max: usize,
    count: CachePadded<AtomicUsize>,
    started: CachePadded<AtomicUsize>,
    followed: CachePadded<AtomicUsize>,

    spin_limit: SpinLimit,
    yield_limit: YieldLimit,
}

#[derive(Debug)]
pub struct BarrierRef<'a> {
    init: &'a BarrierInit,
    lsense: bool,
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

            count: CachePadded::new(AtomicUsize::new(num_threads)),
            started: CachePadded::new(AtomicUsize::new(0)),
            followed: CachePadded::new(AtomicUsize::new(0)),

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
        _ = tid;
        BarrierRef {
            init: self,
            lsense: false,
        }
    }
}

pub static COUNTER: AtomicU32 = AtomicU32::new(0);

impl BarrierRef<'_> {
    #[inline]
    pub fn num_threads(&self) -> usize {
        self.init.max
    }

    #[inline(never)]
    pub fn wait(&mut self) -> BarrierWaitResult {
        self.lsense = !self.lsense;

        if self.init.count.fetch_sub(1, AcqRel) == 1 {
            self.init.followed.store(0, Release);
            self.init.waiting_for_leader.store(true as u32, Release);
            self.init.count.store(self.init.max, Release);
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

                let started = self.init.started.load(Acquire);
                let do_wait = started != 0 && started != self.init.max;
                // let do_wait = wait.is_completed();

                if do_wait {
                    atomic_wait::wait(&*self.init.gsense, (!self.lsense) as u32);
                } else {
                    wait.snooze();
                }
            }
            self.init.followed.fetch_add(1, Release);
            BarrierWaitResult::Follower
        }
    }

    #[inline]
    pub fn lead(&self) {
        self.init.started.store(0, Release);
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

            let followed = self.init.followed.load(Acquire);

            let do_wait = followed != 0 && followed != self.init.max;
            // let do_wait = wait.is_completed();

            if do_wait {
                atomic_wait::wait(&self.init.waiting_for_leader, true as u32);
            } else {
                wait.snooze();
            }
        }
        self.init.started.fetch_add(1, Release);
    }
}
