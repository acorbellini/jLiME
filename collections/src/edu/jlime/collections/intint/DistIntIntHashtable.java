package edu.jlime.collections.intint;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

import edu.jlime.collections.hash.SimpleIntIntHash;
import edu.jlime.jd.ClientCluster;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;
import edu.jlime.jd.job.ResultManager;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntIntHashMap;

public class DistIntIntHashtable implements Iterable<int[]> {

	private static final int MAX_BATCH = 100000;

	private ConsistentHash hash;

	private String internalHashName;

	private ClientCluster cluster;

	private Logger log = Logger.getLogger(DistIntIntHashtable.class);

	public static class PutOrAddJob implements Job<Void> {

		private static final long serialVersionUID = -8857470082770239354L;

		int k;

		int c;

		String hashName;

		public PutOrAddJob(int k, int c, String hashName) {
			this.k = k;
			this.c = c;
			this.hashName = hashName;
		}

		@Override
		public Void call(JobContext ctx, ClientNode peer) throws Exception {
			SimpleIntIntHash hash = (SimpleIntIntHash) ctx.get(hashName);
			synchronized (hash) {
				hash.adjustOrPutValue(k, c, c);
			}
			return null;
		}
	}

	public static class GetHashJob implements Job<SimpleIntIntHash> {

		private static final long serialVersionUID = 7973008348111358911L;

		private String hashName;

		public GetHashJob(String hashName) {
			this.hashName = hashName;
		}

		@Override
		public SimpleIntIntHash call(JobContext ctx, ClientNode peer)
				throws Exception {
			SimpleIntIntHash hash = (SimpleIntIntHash) ctx.get(hashName);
			if (hash == null) {
				return new SimpleIntIntHash();
			} else {
				return hash;
			}
		}
	}

	public static class ClearInternal implements Job<Boolean> {

		private static final long serialVersionUID = 7490204194065719092L;

		private String internal;

		public ClearInternal(String internalHashName) {
			this.internal = internalHashName;
		}

		@Override
		public Boolean call(JobContext ctx, ClientNode peer) throws Exception {
			SimpleIntIntHash inthash = ((SimpleIntIntHash) ctx.get(internal));
			if (inthash != null) {
				// inthash.clear();
				ctx.remove(internal);
			}
			return true;
		}

	}

	public static class ClearJob implements Job<Boolean> {

		private static final long serialVersionUID = 2478490800822344750L;

		private String hash;

		public ClearJob(String hash) {
			this.hash = hash;
		}

		@Override
		public Boolean call(JobContext ctx, ClientNode peer) throws Exception {
			DistIntIntHashtable disthash = (DistIntIntHashtable) ctx.get(hash);
			if (disthash != null) {
				ctx.getCluster().broadcast(
						new ClearInternal(disthash.getInternalHashName()));
				ctx.remove(hash);
			}
			System.gc();
			return true;
		}
	}

	public static class IntIntHashInitJob implements Job<Boolean> {

		private static final long serialVersionUID = -7866470186540636451L;

		private String hashName;

		public IntIntHashInitJob(String hashName) {
			this.hashName = hashName;
		}

		@Override
		public Boolean call(JobContext ctx, ClientNode peer) throws Exception {
			Logger log = Logger.getLogger(IntIntHashInitJob.class);
			log.info("Instantiting Simple Int Int Hash called " + hashName
					+ " requested from " + peer);
			ctx.putIfAbsent(hashName, new SimpleIntIntHash());
			log.info("Finished Instantiation of IntIntHash called " + hashName
					+ " requested from " + peer);
			return true;
		}
	}

	TIntIntHashMap queue = new TIntIntHashMap();

	public DistIntIntHashtable(ClientCluster cluster2) throws Exception {
		this("DistHash - " + UUID.randomUUID().toString(), cluster2);
		// timer = new Timer("Distributed Hash Table Flush", true);
		// timer.schedule(new TimerTask() {
		//
		// @Override
		// public void run() {
		// synchronized (queue) {
		// flush();
		// }
		// }
		// }, BATCH_TIME, BATCH_TIME);

	}

	public DistIntIntHashtable(String name, ClientCluster iCluster)
			throws Exception {
		hash = new ConsistentHash(iCluster);
		this.internalHashName = "InternalHash - " + name;
		iCluster.broadcast(new IntIntHashInitJob(internalHashName));
		this.cluster = iCluster;
	}

	public void putOrAdd(int k, int c) throws Exception {
		getServerForKey(k).execAsync(new PutOrAddJob(k, c, internalHashName));
	}

	public void batchPutOrAdd(int k, int c) {
		queue.adjustOrPutValue(k, c, c);
		if (queue.size() >= MAX_BATCH)
			flush();
	}

	public void putOrAdd(TIntIntHashMap count) throws Exception {
		log.info("Putting into distributed hash " + count.size() + " pairs.");
		HashMap<ClientNode, TIntArrayList> keysPerServer = resolveServers(count
				.keys());

		for (ClientNode srv : keysPerServer.keySet()) {
			TIntIntHashMap subMap = new TIntIntHashMap();
			TIntIterator it = keysPerServer.get(srv).iterator();
			while (it.hasNext()) {
				int val = it.next();
				subMap.put(val, count.get(val));
			}

			maxSend.acquire();
			srv.execAsync(new MultiPutJob(subMap, internalHashName),
					new ResultManager<Boolean>() {

						@Override
						public void handleException(Exception res,
								String jobID, ClientNode fromID) {
							Logger.getLogger(DistIntIntHashtable.class).error(
									"", res);
							maxSend.release();
						}

						@Override
						public void handleResult(Boolean res, String jobID,
								ClientNode fromID) {
							maxSend.release();
						}
					});
		}
	}

	private HashMap<ClientNode, TIntArrayList> resolveServers(int[] keys) {
		HashMap<ClientNode, TIntArrayList> serverList = new HashMap<>();
		for (int i : keys) {
			ClientNode s = getServerForKey(i);
			if (!serverList.containsKey(s))
				serverList.put(s, new TIntArrayList());
			serverList.get(s).add(i);
		}
		return serverList;
	}

	private ClientNode getServerForKey(int k) {
		// Aqui se deberia proveer algun soporte de virtual nodes, de manera que
		// no se cambie la ubicacion de las claves si cambia la estructura de la
		// red.
		return hash.getServerForKey(k);
	}

	public ClientCluster getCluster() {
		return cluster;
	}

	public static class HashTableIterator implements Iterator<int[]> {

		Iterator<ClientNode> currServerIt;

		Iterator<int[]> hashMapIt = null;

		private DistIntIntHashtable disthash;

		private ClientNode currServer;

		public HashTableIterator(DistIntIntHashtable hash) {
			this.disthash = hash;
			try {
				currServerIt = hash.getCluster().iterator();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		@Override
		public boolean hasNext() {
			Logger log = Logger.getLogger(HashTableIterator.class);
			while (hashMapIt == null || !hashMapIt.hasNext()) {
				if (!currServerIt.hasNext())
					return false;
				currServer = currServerIt.next();
				log.info("Current Server iterated " + currServer);
				try {
					hashMapIt = currServer.exec(
							new GetHashJob(disthash.getInternalHashName()))
							.iterator();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			return true;
		}

		@Override
		public int[] next() {
			return hashMapIt.next();
		}

		@Override
		public void remove() {

		}
	}

	@Override
	public Iterator<int[]> iterator() {
		return new HashTableIterator(this);
	}

	public String getInternalHashName() {
		return internalHashName;
	}

	public synchronized static DistIntIntHashtable get(String hash,
			JobContext env) throws Exception {
		if (env.get(hash) == null)
			env.put(hash, new DistIntIntHashtable(hash, env.getCluster()));

		return (DistIntIntHashtable) env.get(hash);
	}

	public static void delete(String hash, ClientCluster c) throws Exception {
		c.broadcast(new ClearJob(hash));
	}

	public void removeAll(int[] toremove) throws InterruptedException,
			ExecutionException {

		HashMap<ClientNode, TIntArrayList> keysPerServer = resolveServers(toremove);

		ArrayList<Future<Boolean>> list = new ArrayList<>();

		for (ClientNode srv : keysPerServer.keySet()) {
			TIntArrayList toRemove = keysPerServer.get(srv);
			// list.add(srv.execAsyncWithFuture(new
			// RemoveJob(toRemove.toArray(),
			// internalHashName)));
		}

		for (Future<Boolean> future : list) {
			future.get();
		}

	}

	private final int max_send = 5000;

	Semaphore maxSend = new Semaphore(max_send);

	public void flush() {
		if (queue.size() == 0)
			return;
		try {
			putOrAdd(queue);
		} catch (Exception e) {
			e.printStackTrace();
		}
		queue.clear();
	}

	protected Semaphore finished;

	public void waitForTermination() {
		flush();
		while (maxSend.availablePermits() != max_send) {
			try {
				// System.out.println(maxSend.availablePermits());
				// Map<UUID, ResultManager> map = cluster.getLocal().getJD()
				// .getJobMap();
				// synchronized (map) {
				// for (Entry<UUID, ResultManager> e : map.entrySet()) {
				// System.out.println(e.getKey() + " --> " + e.getValue());
				// }
				// }
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
