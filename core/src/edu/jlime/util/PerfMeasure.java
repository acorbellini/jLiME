package edu.jlime.util;

import java.util.HashMap;

public class PerfMeasure {

	public static class PerfTime {
		private Thread creator;

		private long initTime;
		private long sum;
		private int c;
		private int curr;

		private String name;

		private boolean thread;

		public PerfTime(String name, int count, Thread creator, boolean thread) {
			this.name = name;
			this.c = count;
			reset();
			this.creator = creator;
			this.thread = thread;
		}

		private void reset() {
			this.initTime = 0;
			this.curr = 0;
			this.sum = 0;
		}

		public long takeTime(long time) {
			if (thread && creator != Thread.currentThread())
				return -1;

			double d = (sum / (double) c);
			if (curr == c) {
				// Exception e = new Exception();
				System.out.println(name + " (" + Thread.currentThread() + ") :" + d + " ms");
				// e.printStackTrace();
				reset();
			} else {
				sum += (time - initTime);
				curr++;
			}
			return (long) d;

		}

		public void setInit() {
			if (thread && creator != Thread.currentThread())
				return;
			initTime = System.currentTimeMillis();
		}

	}

	private static HashMap<String, PerfTime> global = new HashMap<String, PerfMeasure.PerfTime>();

	public static PerfTime startTimer(String name, int count, boolean perThread) {
		String k = name;
		if (perThread)
			k = getName(name);
		return newTimer(k, count, perThread);
	}

	private static PerfTime newTimer(String k, int count, boolean perThread) {
		PerfTime timer = global.get(k);
		if (timer == null)
			synchronized (global) {
				timer = global.get(k);
				if (timer == null) {
					timer = new PerfTime(k, count, Thread.currentThread(), perThread);
					global.put(k, timer);
				}
			}
		timer.setInit();
		return timer;
	}

	private static String getName(String name) {
		Thread currentThread = Thread.currentThread();
		return name + " - " + currentThread.getName() + " - " + currentThread.getId();
	}

	public static PerfTime startGlobal(int count) {
		return newTimer("global", count, false);
	}

	public static void takeTimeGlobal() {
		long time = System.currentTimeMillis();
		takeTime0("global", time);
	}

	public static void takeTime(String name, boolean perThread) {
		long time = System.currentTimeMillis();
		String k = name;
		if (perThread)
			k = getName(name);
		takeTime0(k, time);
	}

	private static void takeTime0(String k, long time) {
		PerfTime perfTime = global.get(k);
		if (perfTime != null)
			perfTime.takeTime(time);
	}

}
