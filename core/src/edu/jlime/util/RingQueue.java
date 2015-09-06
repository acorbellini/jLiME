package edu.jlime.util;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

public class RingQueue {

	// ArrayBlockingQueue<Object> aux = new ArrayBlockingQueue<Object>(1024);

	private Object[] buffer;

	private volatile int init;

	private volatile int end;

	private AtomicInteger reserved;

	public RingQueue(int s) {
		this.init = 0;
		this.end = 0;
		this.reserved = new AtomicInteger(1);
		this.buffer = new Object[s];
	}

	public RingQueue() {
		this(512);
	}

	private int pos(int i) {
		return ((i % buffer.length) + buffer.length) % buffer.length;
	}

	public void put(Object msg) {
		int currentInit = init;
		int currentEnd = end;
		int currentReserved = reserved.getAndIncrement();
		int cont = 0;
		while (currentReserved - currentInit >= buffer.length
				|| currentReserved - 1 != currentEnd) {
			while (currentInit == init && currentEnd == end) {
				cont++;
				if (cont == 1000) {
					synchronized (this) {
						try {
							wait(0, 10);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						cont = 0;
					}
				}
			}
			currentInit = init;
			currentEnd = end;
		}

		buffer[pos(currentReserved - 1)] = msg;

		end = currentReserved;

		synchronized (this) {
			notifyAll();
		}
	}

	public Object[] take() {
		int currentEnd = end;
		int cont = 0;
		while (init == currentEnd) {
			while (currentEnd == end) {
				cont++;
				if (cont == 1000) {
					synchronized (this) {
						try {
							wait(0, 10);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
					cont = 0;
				}
			}
			currentEnd = end;
		}

		Object[] ret = new Object[currentEnd - init];

		int i = pos(init);
		int f = pos(currentEnd);
		if (f > i) {
			ret = Arrays.copyOfRange(buffer, i, f);
			Arrays.fill(buffer, i, f, null);
		} else if (f == i) {
			ret = Arrays.copyOf(buffer, buffer.length);
			Arrays.fill(buffer, null);
		} else {
			System.arraycopy(buffer, i, ret, 0, buffer.length - i);
			Arrays.fill(buffer, i, buffer.length, null);
			System.arraycopy(buffer, 0, ret, buffer.length - i, f);
			Arrays.fill(buffer, 0, f, null);
		}

		init = currentEnd;
		synchronized (this) {
			notifyAll();
		}
		return ret;
	}

	public Object tryTakeOne() {
		int currentEnd = end;
		// int cont = 0;
		if (init == currentEnd) {
			return null;
			// while (currentEnd == end) {
			// cont++;
			// if (cont == 1000) {
			// synchronized (this) {
			// try {
			// wait(0, 10);
			// } catch (InterruptedException e) {
			// e.printStackTrace();
			// }
			// }
			// cont = 0;
			// }
			// }
			// currentEnd = end;
		}

		// Object[] ret = new Object[currentEnd - init];

		Object ret = buffer[pos(init)];
		init = init + 1;
		// int i = pos(init);
		// int f = pos(currentEnd);
		// if (f > i) {
		// ret = Arrays.copyOfRange(buffer, i, f);
		// Arrays.fill(buffer, i, f, null);
		// } else if (f == i) {
		// ret = Arrays.copyOf(buffer, buffer.length);
		// Arrays.fill(buffer, null);
		// } else {
		// System.arraycopy(buffer, i, ret, 0, buffer.length - i);
		// Arrays.fill(buffer, i, buffer.length, null);
		// System.arraycopy(buffer, 0, ret, buffer.length - i, f);
		// Arrays.fill(buffer, 0, f, null);
		// }
		//
		// init = currentEnd;
		synchronized (this) {
			notifyAll();
		}
		return ret;
	}
}
