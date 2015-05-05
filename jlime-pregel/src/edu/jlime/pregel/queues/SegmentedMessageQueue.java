package edu.jlime.pregel.queues;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import edu.jlime.pregel.messages.PregelMessage;
import edu.jlime.pregel.worker.WorkerTask;

public class SegmentedMessageQueue implements PregelMessageQueue {
	ExecutorService pool;

	PregelMessageQueue[] queue;

	volatile Future<?>[] fut;

	int queue_limit = 0;
	private AtomicInteger taskCounter = new AtomicInteger();

	private WorkerTask task;

	Semaphore maxThreads;

	public SegmentedMessageQueue(WorkerTask wt, int segs, int queuelimit,
			MessageQueueFactory fact, int threads) {
		this.maxThreads = new Semaphore((int) (threads * 1.5f));
		this.task = wt;
		this.queue_limit = queuelimit;
		this.fut = new Future[segs];
		this.queue = new PregelMessageQueue[segs];
		for (int i = 0; i < queue.length; i++) {
			this.queue[i] = fact.getMQ();
		}
		this.pool = Executors.newFixedThreadPool(threads, new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				Thread t = Executors.defaultThreadFactory().newThread(r);
				t.setName("SegmentedQueue Sender Pool for Task "
						+ task.toString() + ", id:" + task.getTaskid());
				return t;
			}
		});
	}

	@Override
	public synchronized void switchQueue() {
		synchronized (taskCounter) {
			while (taskCounter.get() != 0)
				try {
					taskCounter.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
		}
		for (int i = 0; i < queue.length; i++)
			queue[i].switchQueue();
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
		// int count = 0;
		// for (PregelMessageQueue pregelMessageQueue : queue) {
		// if (pregelMessageQueue.readOnlySize() == 0)
		// count++;
		// }
		// System.out.println(count);
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
		int hash = getHash(to);
		final PregelMessageQueue cache = this.queue[hash];
		synchronized (cache) {
			checkSize(hash, cache);
			cache.put(from, to, msg);
		}
	}

	private void checkSize(int queueIndex, final PregelMessageQueue cache) {
		if (cache.currentSize() == queue_limit) {
			if (fut[queueIndex] != null)
				try {
					fut[queueIndex].get();
				} catch (Exception e) {
					e.printStackTrace();
				}

			cache.switchQueue();

			taskCounter.incrementAndGet();
			try {
				maxThreads.acquire();
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			fut[queueIndex] = pool.submit(new Runnable() {
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
					maxThreads.release();
				}
			});
		}
	}

	@Override
	public void putFloat(long from, long to, float val) {
		int hash = getHash(to);
		PregelMessageQueue cache = this.queue[hash];
		synchronized (cache) {
			checkSize(hash, cache);
			cache.putFloat(from, to, val);
		}
	}

	private int getHash(long to) {
		int hash = Math
				.abs((int) ((to * 2147483647l) % ((long) this.queue.length)));
		return hash;
	}

	public void flush(final WorkerTask workerTask) throws Exception {
		final Semaphore waitFlush = new Semaphore(-queue.length + 1);
		for (final PregelMessageQueue pregelMessageQueue : queue) {
			maxThreads.acquire();
			pool.execute(new Runnable() {

				@Override
				public void run() {
					try {
						pregelMessageQueue.flush(workerTask);
					} catch (Exception e) {
						e.printStackTrace();
					}
					maxThreads.release();
					waitFlush.release();
				}
			});

		}
		waitFlush.acquire();
	}

	public void clean() {
		pool.shutdown();
	}

	public void putDouble(long from, long to, double val) {
		int hash = getHash(to);
		PregelMessageQueue cache = this.queue[hash];
		synchronized (cache) {
			checkSize(hash, cache);
			cache.putDouble(from, to, val);
		}
	}

	public Iterator<PregelMessage> getMessages(long currentVertex) {
		int hash = getHash(currentVertex);
		PregelMessageQueue cache = this.queue[hash];
		return cache.getMessages(currentVertex);
	}

}
