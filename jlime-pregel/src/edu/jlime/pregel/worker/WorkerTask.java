package edu.jlime.pregel.worker;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.ClientManager;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.pregel.client.CoordinatorFilter;
import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.pregel.client.SplitFunction;
import edu.jlime.pregel.client.WorkerContext;
import edu.jlime.pregel.coordinator.CoordinatorServer;
import edu.jlime.pregel.coordinator.rpc.Coordinator;
import edu.jlime.pregel.coordinator.rpc.CoordinatorBroadcast;
import edu.jlime.pregel.coordinator.rpc.CoordinatorFactory;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.graph.rpc.Graph;
import edu.jlime.pregel.worker.rpc.Worker;
import edu.jlime.pregel.worker.rpc.WorkerBroadcast;
import edu.jlime.pregel.worker.rpc.WorkerFactory;
import gnu.trove.set.hash.TLongHashSet;

public class WorkerTask {

	private static int LIMIT_CACHE = 10000;

	ExecutorService pool;

	volatile Future<?> fut = null;

	volatile MessageQueue cache;

	List<PregelMessage> cacheBroadcast = new ArrayList<PregelMessage>(
			LIMIT_CACHE);

	private Graph graph;

	private UUID taskid;

	private VertexFunction f;

	private WorkerImpl worker;

	protected Logger log = Logger.getLogger(WorkerTask.class);

	// private TLongHashSet halted = new TLongHashSet();

	private int currentStep;

	private MessageQueue queue;

	private PregelConfig config;

	private ClientManager<Coordinator, CoordinatorBroadcast> coordMgr;

	private ClientManager<Worker, WorkerBroadcast> workerMgr;

	private SplitFunction split;

	private AtomicInteger taskCounter = new AtomicInteger();

	private ExecutorService vertexPool;

	public WorkerTask(WorkerImpl w, RPCDispatcher rpc, Peer client,
			final VertexFunction func, final UUID taskID, PregelConfig config) {
		this.pool = Executors.newFixedThreadPool(config.getThreads(),
				new ThreadFactory() {
					@Override
					public Thread newThread(Runnable r) {
						Thread t = Executors.defaultThreadFactory()
								.newThread(r);
						t.setName("Sender Pool for Task " + func.toString()
								+ ", id:" + taskID);
						return t;
					}
				});

		this.vertexPool = Executors.newFixedThreadPool(config.getThreads(),
				new ThreadFactory() {
					@Override
					public Thread newThread(Runnable r) {
						Thread t = Executors.defaultThreadFactory()
								.newThread(r);
						t.setName("Vertex Pool for Task " + func.toString()
								+ ", id:" + taskID);
						return t;
					}
				});

		this.graph = config.getGraph();
		this.worker = w;
		this.coordMgr = rpc.manage(new CoordinatorFactory(rpc,
				CoordinatorServer.COORDINATOR_KEY), new CoordinatorFilter(),
				client);
		this.workerMgr = rpc.manage(new WorkerFactory(rpc,
				WorkerServer.WORKER_KEY), new WorkerFilter(), client);
		this.taskid = taskID;
		this.config = config;
		this.queue = new MessageQueue(config.getMerger());
		this.cache = new MessageQueue(config.getMerger());
		this.f = func;
	}

	public void queueVertexData(PregelMessage msg) {
		// if (halted.contains(msg.getTo()))
		// return;
		queue.put(msg);
		// putIntoQueue(msg);
	}

	public void nextStep(int superstep, SplitFunction func) throws Exception {
		if (log.isDebugEnabled())
			log.debug("Executing step " + superstep + " on Worker "
					+ worker.getID());

		this.split = func;

		this.currentStep = superstep;

		queue.switchQueue();
	}

	public void execute() throws Exception {

		int size = queue.readOnlySize();

		if (size == 0) {
			if (log.isDebugEnabled())
				log.debug("Queue was empty, finished step " + currentStep
						+ " on Worker " + worker.getID());
			try {
				coordMgr.getFirst()
						.finished(taskid, this.worker.getID(), false);
			} catch (Exception e) {
				e.printStackTrace();
			}
			return;
		}

		// ExecutorService exec = Executors.newFixedThreadPool(
		// config.getThreads(), new ThreadFactory() {
		// @Override
		// public Thread newThread(Runnable r) {
		// Thread t = Executors.defaultThreadFactory()
		// .newThread(r);
		// t.setName("Pregel Worker for task " + taskid);
		// return t;
		// }
		// });

		Semaphore execCount = new Semaphore(config.getThreads() * 2);

		int count = 0;
		TLongHashSet executed = new TLongHashSet();

		Iterator<List<PregelMessage>> it = queue.iterator();

		while (it.hasNext()) {
			List<PregelMessage> list = it.next();
			long vid = list.get(0).getTo();
			count += list.size();

			printCompleted(size, count);

			executed.add(vid);

			execVertex(execCount, vid, list);
		}

		// exec.shutdown();
		// exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

		synchronized (taskCounter) {
			while (taskCounter.get() != 0)
				taskCounter.wait();
		}

		if (log.isDebugEnabled())
			log.debug("Finished work for step " + currentStep + " on Worker "
					+ worker.getID());

		forceFlushCache();

		if (!cacheBroadcast.isEmpty()) {
			flushBroadcast(cacheBroadcast);
			cacheBroadcast.clear();
		}

		synchronized (taskCounter) {
			while (taskCounter.get() != 0)
				taskCounter.wait();
		}

		coordMgr.getFirst().finished(taskid, this.worker.getID(), true);
	}

	private void forceFlushCache() throws InterruptedException,
			ExecutionException {
		synchronized (this) {
			if (fut != null)
				fut.get();
			cache.switchQueue();
			flushCache();
		}
	}

	private void printCompleted(int total, double currentCount)
			throws Exception {
		int fraction = ((int) (total / (double) 10));
		if ((currentCount % fraction) == 0) {
			double completed = ((currentCount / total) * 100);
			if (log.isDebugEnabled())
				log.debug("Completed work on worker " + worker.getID() + ": "
						+ Math.ceil(completed) + " % ");
		}
	}

	private void execVertex(final Semaphore execCount,
			final Long currentVertex, final List<PregelMessage> messages)
			throws InterruptedException {
		execCount.acquire();
		taskCounter.incrementAndGet();
		vertexPool.execute(new Runnable() {
			@Override
			public void run() {

				try {
					if (log.isDebugEnabled())
						log.debug("Executing function on vertex "
								+ currentVertex);
					f.execute(currentVertex, messages, new WorkerContext(
							WorkerTask.this, currentVertex));
					if (log.isDebugEnabled())
						log.debug("Finished executing function on vertex "
								+ currentVertex);
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					execCount.release();
				}

				taskCounter.decrementAndGet();
				synchronized (taskCounter) {
					taskCounter.notify();
				}
			}
		});
	}

	public Graph getGraph() {
		return graph;
	}

	public synchronized void send(PregelMessage msg) throws Exception {

		if (cache.currentSize() == LIMIT_CACHE) {
			if (fut != null)
				fut.get();

			cache.switchQueue();
			taskCounter.incrementAndGet();
			fut = pool.submit(new Runnable() {
				@Override
				public void run() {
					flushCache();

					taskCounter.decrementAndGet();
					synchronized (taskCounter) {
						taskCounter.notify();
					}

				}
			});
		}

		cache.put(msg);

	}

	private void flushCache() {
		Iterator<List<PregelMessage>> it = cache.iterator();
		try {
			while (it.hasNext()) {
				List<PregelMessage> list = it.next();
				for (PregelMessage msg : list) {
					Peer peer = split
							.getPeer(msg.getTo(), workerMgr.getPeers());
					Worker w = workerMgr.get(peer);
					try {
						w.sendMessage(msg, taskid);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// public void setHalted(Long v) {
	// synchronized (halted) {
	// this.halted.add(v);
	// }
	// }

	public Integer getSuperStep() {
		return currentStep;
	}

	public Double getAggregatedValue(Long v, String k) throws Exception {
		return coordMgr.getFirst().getAggregatedValue(taskid, v, k);
	}

	public void setAggregatedValue(Long v, String string, double currentVal)
			throws Exception {
		coordMgr.getFirst().setAggregatedValue(taskid, v, string, currentVal);
	}

	public void sendAll(PregelMessage pregelMessage) throws Exception {
		synchronized (cacheBroadcast) {
			if (cacheBroadcast.size() == LIMIT_CACHE) {
				final ArrayList<PregelMessage> arrayList = new ArrayList<>(
						cacheBroadcast);
				taskCounter.incrementAndGet();
				pool.execute(new Runnable() {
					@Override
					public void run() {
						try {
							flushBroadcast(arrayList);
						} catch (Exception e) {
							e.printStackTrace();
						}

						taskCounter.decrementAndGet();
						synchronized (taskCounter) {
							taskCounter.notify();
						}

					}
				});
				cacheBroadcast.clear();
			}
			cacheBroadcast.add(pregelMessage);
		}
	}

	private void flushBroadcast(List<PregelMessage> arrayList) throws Exception {

		for (Long vid : getGraph().vertices()) {
			for (PregelMessage b : arrayList) {
				PregelMessage send = new PregelMessage(b.getFrom(), vid, b.v);
				Peer peer = split.getPeer(vid, workerMgr.getPeers());
				try {
					workerMgr.get(peer).sendMessage(send, taskid);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

	}

	public void sendMessages(List<PregelMessage> value) {
		for (PregelMessage m : value) {
			queueVertexData(m);
		}

	}
}
