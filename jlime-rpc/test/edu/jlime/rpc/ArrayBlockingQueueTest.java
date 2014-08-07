package edu.jlime.rpc;

import java.util.concurrent.ArrayBlockingQueue;

public class ArrayBlockingQueueTest {

	void algo() {

	}

	public static void main(String[] args) throws InterruptedException {

		Integer i1 = new Integer(5);
		Integer i2 = new Integer(5);

		if (i1 == i2)
			System.out.println("Son iguales");
		else
			System.out.println("Son distintos");

		String h1 = "hola";
		String h2 = "hola";
		if (h1 == "hola")
			System.out.println("Son iguales");
		else
			System.out.println("Son distintos");
		if (h1 == h2)
			System.out.println("Son iguales");
		else
			System.out.println("Son distintos");

		if ("hola" == "hola")
			System.out.println("Son iguales");
//		else
//			System.out.println("Son distintos");

		if (new String("hola") == new String("hola"))
			System.out.println("Son iguales");
		else
			System.out.println("Son distintos");

		final ArrayBlockingQueue<Integer> queue = new ArrayBlockingQueue<>(32);
		final int ITER = 10000;
		final long i = System.nanoTime();
		new Thread() {
			int count = 0;

			public void run() {
				while (true) {
					try {
						Integer list = queue.take();
						// System.out.println(list);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}

					count++;
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
		while (iter < ITER)
			queue.put(new Integer(iter++));

	}
}
