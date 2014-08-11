package edu.jlime.rpc;

import java.io.IOException;

import edu.jlime.util.RingQueue;

public class RingQueueTestMultiThread {

	private static int max_threads = 4;

	public static void main(String[] args) throws IOException {
		// System.in.read();
		final RingQueue queue = new RingQueue();
		// final RingQueueAtomic queue = new RingQueueAtomic();
		final int ITER = 100000;
		final long i = System.nanoTime();
		new Thread() {
			int count = 0;

			public void run() {
				while (true) {
					Object[] list = queue.take();
					try {
						sleep(100);
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}
					for (Object e : list) {
						System.out.println(e);
						count++;
					}
					if (count == max_threads * ITER) {
						long dur = System.nanoTime() - i;
						System.out.println(dur / 1000 / 1000 + "ms");
						System.out.println(dur / ITER + "ns");
						System.exit(0);
					}
				}
			};
		}.start();

		Runnable run = new Runnable() {
			@Override
			public void run() {
				int iter = 0;
				while (iter < ITER) {
					queue.put(new Integer(iter++));
				}
			}
		};

		for (int j = 0; j < max_threads; j++) {
			Thread t = new Thread(run);
			t.start();
		}

	}
}
