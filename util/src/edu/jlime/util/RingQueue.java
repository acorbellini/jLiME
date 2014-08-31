package edu.jlime.util;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

public class RingQueue {

	// ArrayBlockingQueue<Object> aux = new ArrayBlockingQueue<Object>(1024);

	private Object[] buffer;

	// private boolean[] used;

	private volatile int init;

	private volatile int end;

	private AtomicInteger reserved;

	public RingQueue(int s) {
		this.init = 0;
		this.end = 0;
		this.reserved = new AtomicInteger(1);
		this.buffer = new Object[s];
		// this.used = new boolean[len];
	}

	public RingQueue() {
		this(2048);
	}

	private int pos(int i) {
		return ((i % buffer.length) + buffer.length) % buffer.length;
	}

	// private boolean isEmpty() {
	// return aux.isEmpty();
	// int currentEnd = end;
	// return init == currentEnd;
	// }

	public void put(Object msg) {
		// try {
		// aux.put(msg);
		// } catch (InterruptedException e1) {
		//
		// e1.printStackTrace();
		// }
		// return;

		int currentInit = init;
		int currentEnd = end;
		int currentReserved = reserved.getAndIncrement();

		while (currentReserved - currentInit >= buffer.length
				|| currentReserved - 1 != currentEnd) {
			// int count = 0;
			while (currentInit == init && currentEnd == end) {
				try {
					synchronized (this) {
						wait(0, 100);
					}
					// Thread.sleep(1);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				// count++;
				// if (count > MAX_SPIN) {
				// // lockFull.lock();
				// // try {
				// // System.out.println("Se durmio el escritor");
				// // full.await(5, TimeUnit.MILLISECONDS);
				// // } catch (InterruptedException e) {
				// // e.printStackTrace();
				// // }
				// // lockFull.unlock();
				// try {
				// Thread.sleep(1);
				// } catch (InterruptedException e) {
				// e.printStackTrace();
				// }
				// }
			}
			currentInit = init;
			currentEnd = end;
		}

		// endLock.lock();

		// synchronized (buffer) {

		buffer[pos(currentReserved - 1)] = msg;

		end = currentReserved;

		// used[pos(currentReserved)] = true;
		// synchronized (used) {
		// if (currentReserved == end) {
		// do {
		// used[pos(end)] = false;
		// end++;
		// } while (used[pos(end)]);
		// }
		// }
		// }
		// }
		synchronized (this) {
			notifyAll();
		}
		// endLock.unlock();
	}

	public Object[] take() {
		// try {
		// return new Object[] { aux.take() };
		// } catch (InterruptedException e1) {
		// e1.printStackTrace();
		// }
		// return null;
		int currentEnd = end;
		while (init == currentEnd) {
			// int count = 0;
			while (currentEnd == end) {
				synchronized (this) {
					try {
						wait(0, 100);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				// Thread.sleep(1);

				// count++;
				// if (count > MAX_SPIN) {
				// try {
				// Thread.sleep(1);
				// } catch (InterruptedException e) {
				// e.printStackTrace();
				// }
				// }
			}
			currentEnd = end;
		}

		Object[] ret = new Object[currentEnd - init];

		// System.out.println("Leyendo " + (currentEnd - init) +
		// " elementos.");
		// int pos = 0;
		// for (; init < currentEnd; init++) {
		// ret[pos++] = buffer.get(pos(init));
		// buffer.set(pos(init), null);
		// }
		int i = pos(init);
		int f = pos(currentEnd);
		if (f > i) {
			ret = Arrays.copyOfRange(buffer, i, f);
			// synchronized (buffer) {
			Arrays.fill(buffer, i, f, null);
			// Arrays.fill(used, i, f, false);
			// }
		} else if (f == i) {
			ret = Arrays.copyOf(buffer, buffer.length);
			// synchronized (buffer) {
			Arrays.fill(buffer, null);
			// Arrays.fill(used, false);
			// }
		} else {
			// synchronized (buffer) {
			System.arraycopy(buffer, i, ret, 0, buffer.length - i);
			Arrays.fill(buffer, i, buffer.length, null);
			// Arrays.fill(used, i, len, false);
			System.arraycopy(buffer, 0, ret, buffer.length - i, f);
			Arrays.fill(buffer, 0, f, null);
			// Arrays.fill(used, 0, f, false);
			// }
		}

		init = currentEnd;
		synchronized (this) {
			notifyAll();
		}
		return ret;
	}
}
