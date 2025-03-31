use super::SPIN_LIMIT;
use crate::PAUSE_PER_SPIN;

use core::sync::atomic::AtomicU32;
use core::sync::atomic::Ordering::*;
use std::ptr::null_mut;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicPtr};

#[derive(Debug)]
pub struct BarrierInit {
	/// bit layout:
	/// - `0..15`: count
	/// - `16..31`: max
	/// - `31`: global sense
	pub(crate) thread_ids: Box<[AtomicU32]>,

	pub(crate) data: AtomicU32,
}

#[derive(Debug)]
pub struct Barrier {
	pub(crate) init: Arc<BarrierInit>,
	thread_id: u32,
	local_sense: AtomicBool,
}

pub const MASK: u32 = (1u32 << 15) - 1;

impl BarrierInit {
	pub fn new(max_threads: usize) -> Self {
		if max_threads > MASK as usize {
			panic!("maximum supported limit of 32767 threads exceeded");
		}

		Self {
			thread_ids: (0..max_threads as u32).map(AtomicU32::new).collect(),
			data: AtomicU32::new(0),
		}
	}

	pub fn current_nthreads(&self) -> usize {
		((self.data.load(Relaxed) >> 16) & MASK) as usize
	}

	pub fn barrier(self: Arc<Self>) -> Option<Barrier> {
		loop {
			let data = self.data.load(Acquire);
			let max = (data >> 16) & MASK;
			if max == self.thread_ids.len() as u32 {
				return None;
			}

			if data & MASK == 0 && max > 0 {
				continue;
			}

			if self
				.data
				.compare_exchange_weak(data, data + (1u32 | (1u32 << 16)), Release, Relaxed)
				.is_ok()
			{
				loop {
					for id in &self.thread_ids {
						let v = id.load(Relaxed);
						if v != u32::MAX && id.compare_exchange(v, u32::MAX, Relaxed, Relaxed).is_ok() {
							return Some(Barrier {
								init: self,
								thread_id: v,
								local_sense: AtomicBool::new((data >> 31) & 1 != 0),
							});
						}
					}
				}
			}

			core::hint::spin_loop();
		}
	}
}

impl Barrier {
	#[inline(never)]
	pub fn wait_and_null(&self, ptr: &AtomicPtr<()>, exit: bool) {
		let local_sense = !self.local_sense.load(Relaxed);
		self.local_sense.store(local_sense, Relaxed);
		if exit {
			'give_id: loop {
				for id in &self.init.thread_ids {
					let v = id.load(Relaxed);
					if v == u32::MAX && id.compare_exchange(v, self.thread_id, Relaxed, Relaxed).is_ok() {
						break 'give_id;
					}
				}
			}
		}

		let data = self.init.data.fetch_sub(if exit { 1 | (1 << 16) } else { 1 }, AcqRel);
		let count = data & MASK;
		assert!(count > 0);

		if count == 1 {
			let mut max = (data >> 16) & MASK;
			if exit {
				max -= 1;
			}

			ptr.store(null_mut(), Relaxed);

			self.init.data.store((max | (max << 16)) | ((local_sense as u32) << 31), Release);
			atomic_wait::wake_all(&self.init.data);
		} else if !exit {
			let mut spin = 0u32;
			let max_spin = PAUSE_PER_SPIN.load(Relaxed);
			let limit = SPIN_LIMIT.load(Relaxed) / max_spin;
			loop {
				let data = self.init.data.load(Acquire);

				if data >> 31 == local_sense as u32 {
					break;
				}

				if spin < limit {
					for _ in 0..max_spin {
						core::hint::spin_loop();
					}
					spin += 1;
				} else {
					atomic_wait::wait(&self.init.data, data);
				}
			}
		}
	}

	#[inline]
	pub fn thread_id(&self) -> usize {
		self.thread_id as usize
	}
}
