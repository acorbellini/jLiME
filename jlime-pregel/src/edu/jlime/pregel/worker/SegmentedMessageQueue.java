package edu.jlime.pregel.worker;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class SegmentedMessageQueue implements PregelMessageQueue {
	ExecutorService pool;

	PregelMessageQueue[] queue;
	volatile Future<?>[] fut;
	int queue_limit = 0;
	private AtomicInteger taskCounter = new AtomicInteger();

	private WorkerTask task;

	public SegmentedMessageQueue(WorkerTask wt, int segs, int queuelimit,
			MessageQueueFactory fact) {
		this.task = wt;
		this.queue_limit = queuelimit;
		this.fut = new Future[segs];
		this.queue = new PregelMessageQueue[segs];
		for (int i = 0; i < queue.length; i++) {
			this.queue[i] = fact.getMQ();
		}
		this.pool = Executors.newCachedThreadPool(new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				Thread t = Executors.defaultThreadFactory().newThread(r);
				t.setName("Sender Pool for Task " + task.toString() + ", id:"
						+ task.getTaskid());
				return t;
			}
		});
	}

	@Override
	public synchronized void switchQueue() {
		for (int i = 0; i < queue.length; i++) {
			if (fut[i] != null)
				try {
					fut[i].get();
				} catch (Exception e) {
					e.printStackTrace();
				}
			queue[i].switchQueue();
		}

	}

	@Override
	public int currentSize() {
		int size = 0;
		for (int i = 0; i < queue.length; i++) {
			size += queue[i].currentSize();
		}
		return size;
	}

	@Override
	public int readOnlySize() {
		int size = 0;
		for (int i = 0; i < queue.length; i++) {
			size += queue[i].readOnlySize();
		}
		return size;
	}

	@Override
	public Iterator<List<PregelMessage>> iterator() {
		return new SegmentedIterator(this);
	}

	@Override
	public void put(long from, long to, Object msg) {
		int hash = (int) ((to * 31) % this.queue.length);
		final PregelMessageQueue cache = this.queue[hash];
		synchronized (cache) {
			checkSize(hash, cache);
			cache.put(from, to, msg);
		}
	}

	private void checkSize(int hash, final PregelMessageQueue cache) {
		if (cache.currentSize() == queue_limit) {
			if (fut[hash] != null)
				try {
					fut[hash].get();
				} catch (Exception e) {
					e.printStackTrace();
				}

			cache.switchQueue();

			taskCounter.incrementAndGet();

			fut[hash] = pool.submit(new Runnable() {
				@Override
				public void run() {
					try {
						cache.flush(task);
					} catch (Exception e) {
						e.printStackTrace();
					}

					taskCounter.decrementAndGet();
					synchronized (taskCounter) {
						taskCounter.notify();
					}

				}
			});
		}
	}

	@Override
	public void putFloat(long from, long to, float val) {
		int hash = (int) ((to * 31) % this.queue.length);
		final PregelMessageQueue cache = this.queue[hash];
		synchronized (cache) {
			checkSize(hash, cache);
			cache.putFloat(from, to, val);
		}
	}

	public void flush(WorkerTask workerTask) throws Exception {
		synchronized (taskCounter) {
			while (taskCounter.get() != 0)
				try {
					taskCounter.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
		}
		for (PregelMessageQueue pregelMessageQueue : queue) {
			pregelMessageQueue.flush(workerTask);
		}
	}

	public void clean() {
		pool.shutdown();
	}

}
