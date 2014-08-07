package edu.jlime.util;

import java.util.Arrays;

public class RingQueue {

	private Object[] buffer;

	private boolean[] used;

	private volatile int init;

	private volatile int end;

	private volatile int reserved;

	private int len;

	public RingQueue() {
		this.init = 0;
		this.end = 0;
		this.reserved = 0;
		this.len = 1024;
		this.buffer = new Object[len];
		this.used = new boolean[len];
	}

	public int pos(int i) {
		return (i % len + len) % len;
	}

	public boolean isEmpty() {
		return true;
	}

	public synchronized int getAndIncrementReserved() {
		return reserved++;
	}

	public void add(Object msg) {
		int currentInit = init;
		int currentReserved = getAndIncrementReserved();

		while (currentReserved - currentInit >= len) {
			// int count = 0;
			while (currentInit == init
			// && currentEnd == end
			) {
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
			// currentEnd = end;
		}

		// endLock.lock();

		// synchronized (buffer) {
		buffer[pos(currentReserved)] = msg;
		used[pos(currentReserved)] = true;
		if (currentReserved == end) {
			synchronized (used) {
				if (currentReserved == end) {
					do {
						used[pos(end)] = false;
						end++;
					} while (used[pos(end)]);
				}
			}
		}
		// }
		synchronized (this) {
			notifyAll();
		}

		// endLock.unlock();
	}

	public Object[] get() {
		int currentEnd = end;
		while (init == currentEnd) {
			// int count = 0;
			while (currentEnd == end && init == currentEnd) {
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

		// System.out.println("Leyendo " + (currentEnd - init) + " elementos.");
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
			Arrays.fill(used, i, f, false);
			// }
		} else if (f == i) {
			ret = Arrays.copyOf(buffer, len);
			// synchronized (buffer) {
			Arrays.fill(buffer, null);
			Arrays.fill(used, false);
			// }
		} else {
			ret = new Object[currentEnd - init];
			// synchronized (buffer) {
			System.arraycopy(buffer, i, ret, 0, len - i);
			Arrays.fill(buffer, i, len, null);
			Arrays.fill(used, i, len, false);
			System.arraycopy(buffer, 0, ret, len - i, f);
			Arrays.fill(buffer, 0, f, null);
			Arrays.fill(used, 0, f, false);
			// }
		}

		init = currentEnd;
		synchronized (this) {
			notifyAll();
		}
		return ret;
	}
}
