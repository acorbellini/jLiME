package edu.jlime.pregel.worker;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;

public class WorkerTask {

	Worker[] workers;

	// volatile Future<?>[] fut;

	private SegmentedMessageQueue cache;

	private PregelMessageQueue cacheBroadcast;

	private Graph graph;

	private int taskid;

	private VertexFunction f;

	private WorkerImpl worker;

	protected Logger log = Logger.getLogger(WorkerTask.class);

	private int currentStep;

	private SegmentedMessageQueue queue;

	private PregelConfig config;

	private ClientManager<Coordinator, CoordinatorBroadcast> coordMgr;

	private ClientManager<Worker, WorkerBroadcast> workerMgr;

	private SplitFunction split;

	private ExecutorService vertexPool;

	private TLongArrayList vList;
	//
	private TLongArrayList currentSplit = new TLongArrayList();

	private AtomicInteger vertexCounter = new AtomicInteger(0);

	public WorkerTask(WorkerImpl w, RPCDispatcher rpc, Peer client,
			final VertexFunction func, long[] vList, final int taskID2,
			PregelConfig config) throws Exception {
		log.info("Creating task with ID " + taskID2);
		// cacheBroadcast = new
		// ArrayList<PregelMessage>(config.getQueueLimit());
		// fut = new Future[config.getSegments()];

		this.vertexPool = Executors.newFixedThreadPool(config.getThreads(),
				new ThreadFactory() {
					@Override
					public Thread newThread(Runnable r) {
						Thread t = Executors.defaultThreadFactory()
								.newThread(r);
						t.setName("Vertex Pool for Task " + func.toString()
								+ ", id:" + taskID2);
						return t;
					}
				});

		this.graph = config.getGraph();

		// this.vList = new TLongArrayList();
		// try {
		// for (Long vid : graph.vertices()) {
		// vList.add(vid);
		// }
		// } catch (Exception e) {
		// e.printStackTrace();
		// }

		this.worker = w;
		this.coordMgr = rpc.manage(new CoordinatorFactory(rpc,
				CoordinatorServer.COORDINATOR_KEY), new CoordinatorFilter(),
				client);
		this.workerMgr = rpc.manage(new WorkerFactory(rpc,
				WorkerServer.WORKER_KEY), new WorkerFilter(), client);
		this.taskid = taskID2;

		if (config.isExecuteOnAll()) {
			log.info("Creating vertex list for whole graph.");
			TLongArrayList gatherVList = new TLongArrayList();
			for (Long vid : config.getGraph().vertices()) {
				gatherVList.add(vid);
			}
			this.vList = gatherVList;
		} else
			this.vList = new TLongArrayList(vList);

		this.setConfig(config);
		MessageMerger merger = config.getMerger();
		MessageQueueFactory fact = merger != null ? merger.getFactory()
				: MessageQueueFactory.simple(null);

		this.queue = new SegmentedMessageQueue(this, config.getSegments(),
				config.getQueueLimit(), fact);
		this.cache = new SegmentedMessageQueue(this, config.getSegments(),
				config.getQueueLimit(), fact);
		this.cacheBroadcast = fact.getMQ();

		// this.broadcast_queue = new MessageQueue(config.getMerger());

		// this.cache = new PregelMessageQueue[config.getSegments()];
		// for (int i = 0; i < cache.length; i++) {
		// this.cache[i] = new MessageQueue(config.getMerger());
		// }
		// } else {
		// // this.queue = new HashedMessageQueue(config.getMerger());
		// this.queue = new PregelMessageQueue[config.getSegments()];
		// for (int i = 0; i < queue.length; i++) {
		// this.queue[i] = new HashedMessageQueue(config.getMerger());
		// }
		//
		// // this.broadcast_queue = new
		// // HashedMessageQueue(config.getMerger());
		//
		// this.cache = new PregelMessageQueue[config.getSegments()];
		// for (int i = 0; i < cache.length; i++) {
		// this.cache[i] = new HashedMessageQueue(config.getMerger());
		// }
		//
		// }
		this.f = func;
	}

	public void queueVertexData(long from, long to, Object val) {
		this.queue.put(from, to, val);
		// int hash = (int) ((msg.getTo() * 31) % this.queue.length);
		// final PregelMessageQueue cache = this.queue[hash];
		// cache.put(msg.getTo(), false, msg);
	}

	public void queueFloatVertexData(long from, long to, float val) {
		this.queue.putFloat(from, to, val);
		// int hash = (int) ((msg.getTo() * 31) % this.queue.length);
		// final PregelMessageQueue cache = this.queue[hash];
		// cache.put(msg.getTo(), false, msg);
	}

	public void queueBroadcastVertexData(long from, Object val) {
		TLongIterator it = currentSplit.iterator();
		while (it.hasNext()) {
			long vid = it.next();
			queue.put(from, vid, val);
			// int hash = (int) ((vid * 31) % this.queue.length);
			// final PregelMessageQueue cache = this.queue[hash];
			// cache.put(vid, true, msg);
		}
	}

	public void queueBroadcastFloatVertexData(long from, float val) {
		TLongIterator it = currentSplit.iterator();
		while (it.hasNext()) {
			long vid = it.next();
			queue.putFloat(from, vid, val);
			// int hash = (int) ((vid * 31) % this.queue.length);
			// final PregelMessageQueue cache = this.queue[hash];
			// cache.put(vid, true, msg);
		}
	}

	public void nextStep(int superstep, SplitFunction func) throws Exception {
		log.info("Executing step " + superstep + " on Worker " + worker.getID());

		this.split = func;

		Peer[] peers = this.split.getPeers();

		workers = new Worker[peers.length];
		for (int i = 0; i < peers.length; i++) {
			workers[i] = workerMgr.get(peers[i]);
		}

		this.currentStep = superstep;

		queue.switchQueue();
		// for (int i = 0; i < queue.length; i++)
		// queue[i].switchQueue();

		// broadcast_queue.switchQueue();

		// It's done twice, called with -1 and 0.
		log.info("Loading local slice of vertices from " + vList.size()
				+ " vertices.");

		currentSplit.clear();
		Peer localPeer = workerMgr.getLocalPeer();
		List<Peer> peers2 = workerMgr.getPeers();
		TLongIterator it = vList.iterator();
		while (it.hasNext()) {
			long vid = it.next();
			if (split.getPeer(vid, peers2).equals(localPeer))
				currentSplit.add(vid);
		}
		log.info("Loaded slice " + currentSplit.size() + " vertices.");
	}

	public void execute() throws Exception {
		int size = queue.readOnlySize();

		if (size == 0) {
			// if (log.isDebugEnabled())
			log.info("Queue was empty, finished step " + currentStep
					+ " on Worker " + worker.getID());
			try {
				coordMgr.getFirst().finished(getTaskid(), this.worker.getID(),
						false);
			} catch (Exception e) {
				e.printStackTrace();
			}
			return;
		}

		log.info("Starting Execution on worker " + worker.getID() + " for "
				+ size + " messages.");

		Semaphore execCount = new Semaphore(getConfig().getThreads() * 2);

		int count = 0;
		Iterator<List<PregelMessage>> it = queue.iterator();

		while (it.hasNext()) {
			List<PregelMessage> list = it.next();
			long vid = list.get(0).getTo();
			count++;

			printCompleted(size, count);

			// executed.add(vid);

			// Iterator<PregelMessage> broadcast = broadcast_queue
			// .readOnlyIterator();
			// while (broadcast.hasNext()) {
			// PregelMessage pregelMessage = (PregelMessage) broadcast
			// .next();
			// list.add(pregelMessage);
			// }
			execVertex(execCount, vid, list);
		}

		synchronized (vertexCounter) {
			while (vertexCounter.get() != 0)
				try {
					vertexCounter.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
		}

		// exec.shutdown();
		// exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

		// if (log.isDebugEnabled())
		log.info("Finished work for step " + currentStep + " on Worker "
				+ worker.getID());

		cache.flush(this);

		cacheBroadcast.flush(this);

		coordMgr.getFirst().finished(getTaskid(), this.worker.getID(), true);
	}

	// private void forceFlushCache() throws InterruptedException,
	// ExecutionException {
	// synchronized (this) {
	// for (int i = 0; i < cache.length; i++) {
	// if (fut[i] != null)
	// fut[i].get();
	// cache[i].switchQueue();
	// flushCache(cache[i]);
	// }
	//
	// }
	// }

	private void printCompleted(int total, double currentCount)
			throws Exception {
		int fraction = ((int) (total / (double) 10));
		if ((currentCount % fraction) == 0) {
			double completed = ((currentCount / total) * 100);
			// if (log.isDebugEnabled())
			log.info("Completed work on worker " + worker.getID() + ": "
					+ Math.ceil(completed) + " % ");
		}
	}

	private void execVertex(final Semaphore execCount,
			final Long currentVertex, final List<PregelMessage> messages)
			throws InterruptedException {
		execCount.acquire();
		vertexCounter.incrementAndGet();
		vertexPool.execute(new Runnable() {
			@Override
			public void run() {

				try {
					if (log.isDebugEnabled())
						log.debug("Executing function on vertex "
								+ currentVertex);
					WorkerContext ctx = new WorkerContext(WorkerTask.this,
							currentVertex);
					f.execute(currentVertex, messages, ctx);
					if (log.isDebugEnabled())
						log.debug("Finished executing function on vertex "
								+ currentVertex);
					// ctx.finished();
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					execCount.release();
				}

				vertexCounter.decrementAndGet();
				synchronized (vertexCounter) {
					vertexCounter.notify();
				}
			}
		});
	}

	public Graph getGraph() {
		return graph;
	}

	public void sendAll(long from, Object msg) throws Exception {

		cacheBroadcast.put(from, -1, msg);

		// synchronized (cacheBroadcast) {
		// if (cacheBroadcast.size() == config.getQueueLimit()) {
		// final ArrayList<PregelMessage> arrayList = new ArrayList<>(
		// cacheBroadcast);
		// taskCounter.incrementAndGet();
		// pool.execute(new Runnable() {
		// @Override
		// public void run() {
		// try {
		// flushBroadcast(arrayList);
		// } catch (Exception e) {
		// e.printStackTrace();
		// }
		//
		// taskCounter.decrementAndGet();
		// synchronized (taskCounter) {
		// taskCounter.notify();
		// }
		//
		// }
		// });
		// cacheBroadcast.clear();
		// }
		// cacheBroadcast.add(pregelMessage);
		// }
	}

	public void send(long from, long to, Object val) throws Exception {

		cache.put(from, to, val);

		// int hash = (int) ((msg.getTo() * 31) % this.cache.length);
		// final PregelMessageQueue cache = this.cache[hash];
		// synchronized (cache) {
		// if (cache.currentSize() == config.getQueueLimit()) {
		// if (fut[hash] != null)
		// fut[hash].get();
		//
		// cache.switchQueue();
		//
		// taskCounter.incrementAndGet();
		// fut[hash] = pool.submit(new Runnable() {
		// @Override
		// public void run() {
		// flushCache(cache);
		//
		// taskCounter.decrementAndGet();
		// synchronized (taskCounter) {
		// taskCounter.notify();
		// }
		//
		// }
		// });
		// }
		//
		// cache.put(msg.getFrom(), msg.getTo(), msg.getV());
		// }
	}

	private void flushCache(PregelMessageQueue cache) throws Exception {
		cache.flush(this);
		// CacheIterator it = cache.readOnlyIterator();
		// while (it.hasNext()) {
		// Worker w = workers[split.hash(last.getTo())];
		// try {
		// w.sendMessage(last, taskid);
		// } catch (Exception e) {
		// e.printStackTrace();
		// }
		//
		// }
	}

	public Worker getWorker(long vid) {
		return workers[split.hash(vid)];
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
		return coordMgr.getFirst().getAggregatedValue(getTaskid(), v, k);
	}

	public void setAggregatedValue(Long v, String string, double currentVal)
			throws Exception {
		coordMgr.getFirst().setAggregatedValue(getTaskid(), v, string,
				currentVal);
	}

	private void flushBroadcast(PregelMessageQueue broadcastQueue)
			throws Exception {
		broadcastQueue.flush(this);
		// MessageMerger merger = config.getMerger();
		// if (merger != null) {
		// PregelMessage toSend = arrayList.get(0).getCopy();
		// toSend.setFrom(-1);
		// for (PregelMessage pregelMessage : arrayList) {
		// merger.merge(toSend, pregelMessage, toSend);
		// }
		// workerMgr.broadcast().sendMessage(toSend, taskid);
		// } else
		// for (PregelMessage b : arrayList) {
		// b.setBroadcast(true);
		// workerMgr.broadcast().sendMessage(b, taskid);
		// }

	}

	public PregelConfig getConfig() {
		return config;
	}

	public void setConfig(PregelConfig config) {
		this.config = config;
	}

	public void cleanup() {
		this.queue.clean();
		this.cache.clean();
		vertexPool.shutdown();
	}

	public void sendAllFloat(long from, long to, float val) throws Exception {
		synchronized (cacheBroadcast) {
			if (cacheBroadcast.currentSize() == config.getQueueLimit()) {
				flushBroadcast(cacheBroadcast);
			}
			cacheBroadcast.putFloat(from, -1, val);
		}
	}

	public void sendFloat(long from, long to, float val) throws Exception {
		synchronized (cache) {
			if (cache.currentSize() == config.getQueueLimit()) {
				flushBroadcast(cache);
			}
			cache.putFloat(from, to, val);
		}
	}

	public void outputObject(long from, long to, Object val) throws Exception {
		if (to != -1)
			getWorker(to).sendMessage(from, to, val, getTaskid());
		else
			getWorker(to).sendBroadcastMessage(from, val, getTaskid());
	}

	public void outputFloat(long from, long to, float value) throws Exception {
		if (to != -1)
			getWorker(to).sendFloatMessage(from, to, value, getTaskid());
		else
			getWorker(to).sendFloatBroadcastMessage(from, value, getTaskid());
	}

	public int getTaskid() {
		return taskid;
	}
}
