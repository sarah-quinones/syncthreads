use super::SPIN_LIMIT;
use core::sync::atomic::AtomicU32;
use core::sync::atomic::Ordering::*;
use std::ptr::null_mut;
use std::sync::atomic::AtomicPtr;

#[derive(Debug)]
pub struct BarrierInit {
	/// bit layout:
	/// - `0..15`: count
	/// - `16..31`: max
	/// - `31`: global sense
	pub(crate) data: AtomicU32,
	pub(crate) arrived: AtomicU32,
	pub(crate) __thread_count__: AtomicU32,
}

#[derive(Debug)]
pub struct Barrier<'a> {
	pub(crate) init: &'a BarrierInit,
	thread_id: u32,
	local_sense: bool,
}

const MASK: u32 = (1u32 << 15) - 1;

impl BarrierInit {
	pub fn new() -> Self {
		Self {
			data: AtomicU32::new(1u32 | (1u32 << 16)),
			arrived: AtomicU32::new(0),
			__thread_count__: AtomicU32::new(1),
		}
	}

	pub fn leader(&self) -> Barrier<'_> {
		Barrier {
			init: self,
			thread_id: 0,
			local_sense: false,
		}
	}

	pub fn current_nthreads(&self) -> usize {
		((self.data.load(Relaxed) >> 16) & MASK) as usize
	}

	pub fn follower(&self) -> Barrier<'_> {
		if self.__thread_count__.fetch_add(1, Relaxed) == MASK {
			panic!("maximum supported limit of 32767 threads exceeded");
		}

		loop {
			let data = self.data.load(Acquire);
			if data & MASK == 0 {
				continue;
			}

			if self
				.data
				.compare_exchange_weak(data, data + (1u32 | (1u32 << 16)), AcqRel, Relaxed)
				.is_ok()
			{
				let max = (data >> 16) & MASK;
				return Barrier {
					init: self,
					thread_id: max,
					local_sense: (data >> 31) & 1 != 0,
				};
			};

			core::hint::spin_loop();
		}
	}
}

impl Barrier<'_> {
	#[inline(never)]
	pub fn wait_and_null(&mut self, ptr: &AtomicPtr<()>) {
		self.local_sense = !self.local_sense;
		let data = self.init.data.fetch_sub(1, AcqRel);

		let count = data & MASK;

		if count == 1 {
			let max = (data >> 16) & MASK;

			self.init.arrived.store(max, Relaxed);
			ptr.store(null_mut(), Relaxed);

			self.init.data.store((data + max - 1) ^ (1u32 << 31), Release);
			atomic_wait::wake_all(&self.init.data);
		} else {
			let mut spin = 0u32;
			loop {
				let data = self.init.data.load(Acquire);

				if (data >> 31 != 0) == self.local_sense {
					break;
				}

				if spin < SPIN_LIMIT.load(Relaxed) {
					spin += 1;
					core::hint::spin_loop();
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
