package edu.jlime.core.volatiletest;

public class VolatileTest implements Runnable {

	public static final long ITERATIONS = 50000;

	public static long s1 = 1;

	public static long s2 = 1;

	// public static volatile long s1 = 1;
	// public static volatile long s2 = 1;

	public static void main(final String[] args) {
		Thread t = new Thread(new VolatileTest());
		t.setDaemon(true);
		t.start();
		long start = System.nanoTime();
		long v1 = s1;
		while (s1 < ITERATIONS) {
			while (v1 != s2) {
				// try {
				// Thread.sleep(0, 50);
				// } catch (InterruptedException e) {
				// e.printStackTrace();
				// }
				// System.out.println(v1+"!="+s2);
			}
			v1 = ++s1;
		}
		long duration = System.nanoTime() - start;
		System.out.println("duration = " + (duration / 1000 / 1000));
	}

	public void run() {
		long v2 = s2;
		while (true) {
			while (v2 == s1) {
				// try {
				// Thread.sleep(0, 50);
				// } catch (InterruptedException e) {
				// e.printStackTrace();
				// }
				// System.out.println(v2+"=="+s1);
			}
			v2 = ++s2;
		}
	}
}
