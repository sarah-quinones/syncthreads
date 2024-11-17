use alloc::sync::Arc;
use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
use std::sync::atomic::AtomicU32;

#[derive(Debug)]
pub struct BarrierInit {
    done: AtomicBool,
    waiting_for_leader: AtomicBool,
    gsense: AtomicBool,
    count: AtomicUsize,
    max: usize,

    parking: AtomicU32,
}
#[derive(Debug)]
pub struct Barrier {
    init: Arc<BarrierInit>,
    lsense: bool,
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
    pub fn new(num_threads: usize) -> Self {
        Self {
            done: AtomicBool::new(false),
            waiting_for_leader: AtomicBool::new(false),
            count: AtomicUsize::new(num_threads),
            gsense: AtomicBool::new(false),
            max: num_threads,

            parking: AtomicU32::new(0),
        }
    }

    #[inline]
    pub fn num_threads(&self) -> usize {
        self.max
    }

    pub fn barrier(self: Arc<Self>) -> Barrier {
        let lsense = false;
        Barrier { init: self, lsense }
    }

    pub fn barrier_ref(&self) -> BarrierRef<'_> {
        let lsense = false;
        BarrierRef { init: self, lsense }
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

                if (self.init.count.fetch_sub(1, SeqCst)) == 1 {
                    let max = self.init.max;
                    self.init.waiting_for_leader.store(true, SeqCst);
                    self.init.count.store(max, SeqCst);
                    self.init.gsense.store(self.lsense, SeqCst);

                    if cfg!(miri) {
                        atomic_wait::wake_all(atomic);
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
                    let wait = crossbeam::utils::Backoff::new();
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
                                atomic_wait::wait(atomic, 1);
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
                    atomic_wait::wake_all(atomic);
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

                let wait = crossbeam::utils::Backoff::new();
                while self.init.waiting_for_leader.load(SeqCst) {
                    if wait.is_completed() {
                        if cfg!(miri) {
                            atomic_wait::wait(atomic, 1);
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

impl_barrier!(Barrier);
impl_barrier!(BarrierRef<'_>);
