package edu.jlime.pregel.worker;

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
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

public class WorkerTask {

	private static int LIMIT_CACHE = 5000;

	private static final int QUEUE_SIZE = 2048;

	private static final float QUEUE_FACTOR = 0.75f;

	ForkJoinPool pool = new ForkJoinPool();

	// HashMap<Worker, HashMap<Long, List<PregelMessage>>> cached = new
	// HashMap<>(
	// QUEUE_SIZE, QUEUE_FACTOR);
	//
	// HashMap<Long, List<PregelMessage>> cachedBroadcast = new HashMap<>(
	// QUEUE_SIZE, QUEUE_FACTOR);

	List<PregelMessage> cache = new ArrayList<PregelMessage>(LIMIT_CACHE);

	List<PregelMessage> cacheBroadcast = new ArrayList<PregelMessage>(
			LIMIT_CACHE);

	private int threads = 2;

	TLongHashSet modified = new TLongHashSet();

	private Graph graph;

	private UUID taskid;

	private RPCDispatcher rpc;

	private VertexFunction f;

	private WorkerImpl worker;

	protected Logger log = Logger.getLogger(WorkerTask.class);

	private TLongHashSet halted = new TLongHashSet();

	private int currentStep;

	private volatile TLongObjectHashMap<List<PregelMessage>> queue = new TLongObjectHashMap<>(
			QUEUE_SIZE, QUEUE_FACTOR);

	private TLongObjectHashMap<List<PregelMessage>> current;

	// private volatile List<PregelMessage> broadcast = new ArrayList<>();

	// private List<PregelMessage> currentBroadcast;

	private PregelConfig config;

	private ClientManager<Coordinator, CoordinatorBroadcast> coordMgr;

	private ClientManager<Worker, WorkerBroadcast> workerMgr;

	private SplitFunction split;

	public WorkerTask(WorkerImpl w, RPCDispatcher rpc, Peer client,
			VertexFunction func, UUID taskID, PregelConfig config) {
		this.graph = config.getGraph();
		this.worker = w;
		this.coordMgr = rpc.manage(new CoordinatorFactory(rpc,
				CoordinatorServer.COORDINATOR_KEY), new CoordinatorFilter(),
				client);
		this.workerMgr = rpc.manage(new WorkerFactory(rpc,
				WorkerServer.WORKER_KEY), new WorkerFilter(), client);
		this.taskid = taskID;
		this.config = config;
		this.threads = config.getThreads();
		this.f = func;
	}

	private void putIntoQueue(PregelMessage pregelMessage) {
		List<PregelMessage> msgList;
		synchronized (queue) {
			msgList = queue.get(pregelMessage.getTo());
		}
		if (msgList == null)
			synchronized (queue) {
				msgList = queue.get(pregelMessage.getTo());
				if (msgList == null) {
					msgList = new LinkedList<>();
					this.queue.put(pregelMessage.getTo(), msgList);
				}
			}
		synchronized (msgList) {
			MessageMerger merger = config.getMerger();
			if (merger != null) {
				if (msgList.isEmpty())
					msgList.add(new PregelMessage(pregelMessage.getFrom(),
							pregelMessage.getTo(), pregelMessage.getV()));
				else {
					PregelMessage m = msgList.get(0);
					m.setV(merger.merge(m.getV(), pregelMessage.getV()));
				}
			} else
				msgList.add(pregelMessage);
		}
	}

	public void queueVertexData(PregelMessage msg) {
		if (halted.contains(msg.getTo()))
			return;

		// if (msg.getTo() == -1)
		// putIntoBroadcast(msg);
		// else
		putIntoQueue(msg);
	}

	// private void putIntoBroadcast(PregelMessage msg) {
	// synchronized (broadcast) {
	// if (config.getMerger() != null) {
	// if (broadcast.isEmpty())
	// broadcast.add(new PregelMessage(msg.getFrom(), -1, msg
	// .getV()));
	// else {
	// PregelMessage pregelMessage = broadcast.get(0);
	// pregelMessage.setV(config.getMerger().merge(
	// pregelMessage.getV(), msg.getV()));
	// }
	// } else
	// broadcast.add(msg);
	//
	// }
	// }

	public void nextStep(int superstep, SplitFunction func) throws Exception {
		if (log.isDebugEnabled())
			log.debug("Executing step " + superstep + " on Worker "
					+ worker.getID());

		this.pool = new ForkJoinPool();

		this.split = func;

		this.currentStep = superstep;

		this.current = queue;

		// this.currentBroadcast = broadcast;

		this.queue = new TLongObjectHashMap<>(QUEUE_SIZE, QUEUE_FACTOR);

		// this.broadcast = new ArrayList<>(broadcast.size());
	}

	public void execute() throws Exception {
		if (current.isEmpty()
		// && currentBroadcast.isEmpty()
		) {
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

		ExecutorService exec = Executors.newFixedThreadPool(threads,
				new ThreadFactory() {
					@Override
					public Thread newThread(Runnable r) {
						Thread t = Executors.defaultThreadFactory()
								.newThread(r);
						t.setName("Pregel Worker for task " + taskid);
						return t;
					}
				});

		Semaphore execCount = new Semaphore(threads * 2);

		int count = 0;
		TLongHashSet executed = new TLongHashSet();
		int size = queue.size();
		TLongObjectIterator<List<PregelMessage>> it = current.iterator();
		while (it.hasNext()) {
			it.advance();
			double currentCount = count++;

			printCompleted(size, currentCount);
			executed.add(it.key());
			execVertex(exec, execCount, it.key(), it.value());
			// ListUtils.concat(it.value()
			// , currentBroadcast));
		}

		// if (!currentBroadcast.isEmpty()) {
		// for (Long v : localGraph.vertices()) {
		// if (!executed.contains(v)) {
		// double currentCount = count++;
		// printCompleted(total, currentCount);
		// execVertex(exec, execCount, v, currentBroadcast);
		// }
		// }
		// }

		exec.shutdown();
		exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

		if (log.isDebugEnabled())
			log.debug("Finished work for step " + currentStep + " on Worker "
					+ worker.getID());

		if (!cache.isEmpty())
			flushCache();

		if (!cacheBroadcast.isEmpty())
			flushBroadcast();

		pool.shutdown();
		try {
			pool.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}

		coordMgr.getFirst().finished(taskid, this.worker.getID(), true);
	}

	private void printCompleted(int total, double currentCount)
			throws Exception {
		int fraction = ((int) (total / (double) 10));
		if ((currentCount % fraction) == 0) {
			double completed = ((currentCount / total) * 100);
			log.info("Completed work on worker " + worker.getID() + ": "
					+ Math.ceil(completed) + " % ");
		}
	}

	private void execVertex(ExecutorService exec, final Semaphore execCount,
			final Long currentVertex, final List<PregelMessage> messages)
			throws InterruptedException {
		modified.add(currentVertex);
		execCount.acquire();
		exec.execute(new Runnable() {
			@Override
			public void run() {

				try {
					if (log.isDebugEnabled())
						log.debug("Executing function on vertex "
								+ currentVertex);
					// PerfMeasure.startTimer("worker", 10000, true);
					f.execute(currentVertex, messages, new WorkerContext(
							WorkerTask.this, currentVertex));
					// PerfMeasure.takeTime("worker", true);
					if (log.isDebugEnabled())
						log.debug("Finished executing function on vertex "
								+ currentVertex);
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					execCount.release();
				}

			}
		});
	}

	public Graph getGraph() {
		return graph;
	}

	public void send(PregelMessage msg) throws Exception {
		synchronized (cache) {
			if (cache.size() == LIMIT_CACHE)
				flushCache();
			cache.add(msg);
		}
	}

	private void flushCache() {
		final ArrayList<PregelMessage> copy = new ArrayList<>(cache);
		pool.execute(new Runnable() {
			@Override
			public void run() {

				for (PregelMessage pregelMessage : copy) {
					Peer peer = split.getPeer(pregelMessage.getTo(),
							workerMgr.getPeers());
					Worker w = workerMgr.get(peer);
					try {
						w.sendMessage(pregelMessage, taskid);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		});
		cache.clear();
	}

	public void setHalted(Long v) {
		synchronized (halted) {
			this.halted.add(v);
		}
	}

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
			if (cacheBroadcast.size() == LIMIT_CACHE)
				flushBroadcast();
			cacheBroadcast.add(pregelMessage);
		}
	}

	private void flushBroadcast() throws Exception {
		final ArrayList<PregelMessage> arrayList = new ArrayList<>(
				cacheBroadcast);
		pool.execute(new Runnable() {
			@Override
			public void run() {
				try {
					for (Long vid : getGraph().vertices()) {
						for (PregelMessage b : arrayList) {
							PregelMessage send = new PregelMessage(b.getFrom(),
									vid, b.v);
							Peer peer = split.getPeer(vid, workerMgr.getPeers());
							try {
								workerMgr.get(peer).sendMessage(send, taskid);
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}

			}
		});

		cacheBroadcast.clear();
	}

	public void sendMessages(List<PregelMessage> value) {
		for (PregelMessage m : value) {
			queueVertexData(m);
		}

	}
}
