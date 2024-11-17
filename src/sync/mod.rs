use crate::backoff::Backoff;
use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
use crossbeam::utils::CachePadded;
use std::sync::atomic::AtomicU32;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct SpinLimit(pub u32);

impl SpinLimit {
    pub fn best(num_threads: usize) -> Self {
        crate::backoff::best_limit_bench(num_threads)
    }
}

impl Default for SpinLimit {
    #[inline]
    fn default() -> Self {
        Self(4)
    }
}

#[derive(Debug)]
struct AtomicCounter {
    counter: CachePadded<AtomicUsize>,
    max: usize,
    sublevel: Vec<AtomicCounter>,
}

#[derive(Debug)]
struct AtomicWait {
    counter: Vec<CachePadded<AtomicU32>>,
}

impl AtomicWait {
    fn new(nthreads: usize) -> Self {
        Self {
            counter: (0..nthreads)
                .map(|_| CachePadded::new(AtomicU32::new(0)))
                .collect(),
        }
    }

    fn wait(&self, tid: usize) {
        atomic_wait::wait(&self.counter[tid], 1);
    }

    fn wake_all(&self) {
        for c in &self.counter {
            atomic_wait::wake_all(&**c);
        }
    }
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
            return self.counter.fetch_sub(1, SeqCst) == 1;
        }
        false
    }

    fn reset(&self) {
        self.counter.store(self.max, SeqCst);
        for c in &self.sublevel {
            c.reset();
        }
    }
}

#[derive(Debug)]
pub struct BarrierInit {
    done: AtomicBool,
    waiting_for_leader: CachePadded<AtomicBool>,
    gsense: CachePadded<AtomicBool>,
    count: AtomicCounter,
    max: usize,

    parking: AtomicWait,
    limit: SpinLimit,
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
    pub fn new(num_threads: usize, limit: SpinLimit) -> Self {
        Self {
            done: AtomicBool::new(false),
            waiting_for_leader: CachePadded::new(AtomicBool::new(false)),
            count: AtomicCounter::new(num_threads),
            gsense: CachePadded::new(AtomicBool::new(false)),
            max: num_threads,

            parking: AtomicWait::new(num_threads),
            limit,
        }
    }

    #[inline]
    pub fn num_threads(&self) -> usize {
        self.max
    }

    pub fn barrier_ref(&self, tid: usize) -> BarrierRef<'_> {
        let lsense = false;
        BarrierRef {
            init: self,
            lsense,
            tid,
        }
    }
}

macro_rules! impl_barrier {
    ($bar: ty) => {
        impl $bar {
            #[inline]
            pub fn num_threads(&self) -> usize {
                self.init.max
            }

            #[inline(never)]
            pub fn wait(&mut self) -> BarrierWaitResult {
                self.lsense = !self.lsense;
                let addr: &BarrierInit = &*self.init;
                let addr = unsafe { core::mem::transmute::<_, usize>(addr as *const BarrierInit) };
                let atomic = &self.init.parking;

                if self.init.count.dec(self.tid, self.init.max) {
                    self.init.waiting_for_leader.store(true, SeqCst);
                    self.init.count.reset();
                    self.init.gsense.store(self.lsense, SeqCst);

                    if cfg!(miri) {
                        atomic.wake_all();
                    } else {
                        unsafe {
                            parking_lot_core::unpark_all(
                                addr,
                                parking_lot_core::DEFAULT_UNPARK_TOKEN,
                            );
                        }
                    }
                    BarrierWaitResult::Leader
                } else {
                    let wait = Backoff::new(self.init.limit.0);
                    loop {
                        let done = self.init.done.load(SeqCst);
                        let keep_going = self.init.gsense.load(SeqCst) != self.lsense;
                        if !keep_going {
                            break;
                        }
                        if done {
                            return BarrierWaitResult::Dropped;
                        }
                        if wait.is_completed() {
                            if cfg!(miri) {
                                atomic.wait(self.tid);
                            } else {
                                unsafe {
                                    parking_lot_core::park(
                                        addr,
                                        || self.init.gsense.load(SeqCst) != self.lsense,
                                        || {},
                                        |_, _| {},
                                        parking_lot_core::DEFAULT_PARK_TOKEN,
                                        None,
                                    );
                                }
                            }
                        } else {
                            wait.snooze();
                        }
                    }
                    BarrierWaitResult::Follower
                }
            }

            #[inline]
            pub fn lead(&self) {
                self.init.waiting_for_leader.store(false, SeqCst);

                let atomic = &self.init.parking;
                let addr: &BarrierInit = &*self.init;
                let addr = unsafe { core::mem::transmute::<_, usize>(addr as *const BarrierInit) };

                if cfg!(miri) {
                    atomic.wake_all();
                } else {
                    unsafe {
                        parking_lot_core::unpark_all(addr, parking_lot_core::DEFAULT_UNPARK_TOKEN);
                    }
                }
            }

            #[inline(never)]
            pub fn follow(&self) {
                let atomic = &self.init.parking;
                let addr: &BarrierInit = &*self.init;
                let addr = unsafe { core::mem::transmute::<_, usize>(addr as *const BarrierInit) };

                let wait = Backoff::new(self.init.limit.0);
                while self.init.waiting_for_leader.load(SeqCst) {
                    if wait.is_completed() {
                        if cfg!(miri) {
                            atomic.wait(self.tid);
                        } else {
                            unsafe {
                                parking_lot_core::park(
                                    addr,
                                    || self.init.waiting_for_leader.load(SeqCst),
                                    || {},
                                    |_, _| {},
                                    parking_lot_core::DEFAULT_PARK_TOKEN,
                                    None,
                                );
                            }
                        }
                    } else {
                        wait.snooze();
                    }
                }
            }
        }
    };
}

impl_barrier!(BarrierRef<'_>);
