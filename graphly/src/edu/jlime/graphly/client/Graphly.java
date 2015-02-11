package edu.jlime.graphly.client;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import edu.jlime.collections.adjacencygraph.get.Dir;
import edu.jlime.core.cluster.DataFilter;
import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.ClientManager;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.graphly.GraphlyStoreNodeI;
import edu.jlime.graphly.GraphlyStoreNodeIBroadcast;
import edu.jlime.graphly.GraphlyStoreNodeIFactory;
import edu.jlime.graphly.server.GraphlyCoordinator;
import edu.jlime.graphly.server.GraphlyCoordinatorBroadcast;
import edu.jlime.graphly.server.GraphlyCoordinatorFactory;
import edu.jlime.graphly.traversal.GraphlyTraversal;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.JobDispatcher;
import edu.jlime.jd.client.Client;
import edu.jlime.rpc.JLiMEFactory;
import gnu.trove.iterator.TLongIntIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.set.hash.TLongHashSet;

public class Graphly implements Closeable {
	private RPCDispatcher rpc;

	ClientManager<GraphlyStoreNodeI, GraphlyStoreNodeIBroadcast> mgr;

	private ConsistentHashing consistenthash;

	private JobDispatcher jobCli;

	ExecutorService svc = Executors.newCachedThreadPool();

	private Graphly(GraphlyCoordinator coord,
			ClientManager<GraphlyStoreNodeI, GraphlyStoreNodeIBroadcast> mgr,
			JobDispatcher jd) throws Exception {
		this.mgr = mgr;
		this.rpc = mgr.getRpc();
		this.jobCli = jd;
		this.consistenthash = coord.getHash();
	}

	public long[] getVertices(final Dir type, final long vertex)
			throws Exception {
		return getClientFor(vertex).getEdges(type, new long[] { vertex });
	}

	private GraphlyStoreNodeI getClientFor(final long vertex) {
		return mgr.get(consistenthash.getNode(vertex));
	}

	public GraphlyTraversal v(long... id) {
		return new GraphlyTraversal(id, this);
	}

	public GraphlyVertex addVertex(long id, String label) throws Exception {
		if (getClientFor(id).addVertex(id, label)) {
			return getVertex(id);
		}
		return null;

	}

	public GraphlyVertex getVertex(long id) {
		return new GraphlyVertex(id, this);
	}

	public void close() {
		try {
			this.rpc.stop();
			this.jobCli.stop();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public String getLabel(long id) throws Exception {
		return getClientFor(id).getLabel(id);
	}

	public void remove(long id) throws Exception {
		getClientFor(id).removeVertex(id);
	}

	public GraphlyEdge addEdge(Long id, Long id2, String label,
			Object... keyValues) throws Exception {
		long where = id;
		long other = id2;
		if (id > id2) {
			where = id2;
			other = id;
		}

		getClientFor(where).addEdge(id, id2, label, keyValues);
		getClientFor(other).addInEdgePlaceholder(id2, id, label);
		return new GraphlyEdge(id, id2, this);
	}

	public static Graphly build(int min) throws Exception {
		Map<String, String> d = new HashMap<>();
		d.put("app", "graphly");
		d.put("type", "client");

		RPCDispatcher rpc = new JLiMEFactory(d,
				new DataFilter("app", "graphly")).build();
		rpc.start();
		return build(rpc, Client.build(min).getJd(), min);

	}

	public static Graphly build(RPCDispatcher rpc, JobDispatcher jd, int min)
			throws Exception {
		ClientManager<GraphlyStoreNodeI, GraphlyStoreNodeIBroadcast> mgr = rpc
				.manage(new GraphlyStoreNodeIFactory(rpc, "graphly"),
						new DataFilter("type", "server"), rpc.getCluster()
								.getLocalPeer());

		ClientManager<GraphlyCoordinator, GraphlyCoordinatorBroadcast> coordMgr = rpc
				.manage(new GraphlyCoordinatorFactory(rpc, "Coordinator"),
						new DataFilter("type", "coord"), rpc.getCluster()
								.getLocalPeer());

		mgr.waitForClient(min);
		coordMgr.waitFirst();
		return new Graphly(coordMgr.getFirst(), mgr, jd);
	}

	public JobDispatcher getJobClient() {
		return jobCli;
	}

	public long[] getEdges(Dir dir, long... vids) throws Exception {
		TLongHashSet ret = new TLongHashSet();
		Map<GraphlyStoreNodeI, TLongArrayList> map = new HashMap<>();
		for (long l : vids) {
			GraphlyStoreNodeI node = getClientFor(l);
			TLongArrayList currList = map.get(node);
			if (currList == null) {
				currList = new TLongArrayList();
				map.put(node, currList);
			}
			currList.add(l);
		}

		if (map.size() == 1) {
			GraphlyStoreNodeI node = map.entrySet().iterator().next().getKey();
			return node.getEdges(dir, vids);
		}

		for (Entry<GraphlyStoreNodeI, TLongArrayList> e : map.entrySet()) {
			svc.execute(new Runnable() {

				@Override
				public void run() {
					try {
						long[] edges = e.getKey().getEdges(dir,
								e.getValue().toArray());
						synchronized (ret) {
							ret.addAll(edges);
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});

		}
		try {
			svc.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		svc.shutdown();

		return ret.toArray();
	}

	public void addEdges(long vid, Dir dir, long[] dests) throws Exception {
		if (dir.equals(Dir.BOTH))
			throw new IllegalArgumentException(
					"Can't load bidirectional edges.");
		getClientFor(vid).addEdges(vid, dir, dests);
	}

	public ConsistentHashing getHash() {
		return consistenthash;
	}

	public ClientNode getClientJobFor(GraphlyStoreNodeI node) throws Exception {
		return jobCli.getCluster().getClientFor(node.getJobAddress());
	}

	public TLongIntHashMap countEdges(Dir dir, long[] vids) throws Exception {
		TLongIntHashMap ret = new TLongIntHashMap();
		Map<GraphlyStoreNodeI, TLongArrayList> map = new HashMap<>();
		for (long l : vids) {
			GraphlyStoreNodeI node = getClientFor(l);
			TLongArrayList currList = map.get(node);
			if (currList == null) {
				currList = new TLongArrayList();
				map.put(node, currList);
			}
			currList.add(l);
		}

		if (map.size() == 1) {
			GraphlyStoreNodeI node = map.entrySet().iterator().next().getKey();
			return node.countEdges(dir, vids);
		}

		for (Entry<GraphlyStoreNodeI, TLongArrayList> e : map.entrySet()) {
			svc.execute(new Runnable() {

				@Override
				public void run() {
					try {
						TLongIntHashMap edges = e.getKey().countEdges(dir,
								e.getValue().toArray());
						synchronized (ret) {
							TLongIntIterator it = edges.iterator();
							while (it.hasNext()) {
								it.advance();
								ret.adjustOrPutValue(it.key(), it.value(),
										it.value());
							}

						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});

		}
		try {
			svc.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		svc.shutdown();

		return ret;
	}

	public Map<GraphlyStoreNodeI, TLongArrayList> hashKeys(long[] data) {
		Map<Peer, TLongArrayList> map = consistenthash.hashKeys(data);
		Map<GraphlyStoreNodeI, TLongArrayList> ret = new HashMap<>();
		for (Entry<Peer, TLongArrayList> e : map.entrySet()) {
			ret.put(mgr.get(e.getKey()), e.getValue());
		}
		return ret;
	}

	public Long getRandomEdge(Long before, Dir d) throws Exception {
		return getClientFor(before).getRandomEdge(before, d);
	}
}
