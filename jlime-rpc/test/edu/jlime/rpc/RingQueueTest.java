package edu.jlime.rpc;

import edu.jlime.util.RingQueue;

public class RingQueueTest {

	public static void main(String[] args) {
		final RingQueue queue = new RingQueue();
		final int ITER = 10000;
		final long i = System.nanoTime();
		new Thread() {
			int count = 0;

			public void run() {
				while (true) {
					Object[] list = queue.get();
					for (Object e : list) {
						System.out.println(e);
						count++;
					}
					if (count == ITER) {
						long dur = System.nanoTime() - i;
						System.out.println(dur / 1000 / 1000 + "ms");
						System.out.println(dur / ITER + "ns");
						System.exit(0);
					}
				}
			};
		}.start();

		int iter = 0;
		while (iter < ITER) {
			queue.add(new Integer(iter++));
		}

	}
}
