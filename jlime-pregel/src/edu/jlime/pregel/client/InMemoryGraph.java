package edu.jlime.pregel.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.ClientManager;
import edu.jlime.core.rpc.PeerFilter;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.Transferible;
import edu.jlime.pregel.graph.PregelGraphLocal;
import edu.jlime.pregel.graph.rpc.Graph;
import edu.jlime.pregel.graph.rpc.GraphBroadcast;
import edu.jlime.pregel.graph.rpc.GraphFactory;

public class InMemoryGraph implements Graph, Transferible {

	private static final int LIMIT = 10000;

	private transient ClientManager<Graph, GraphBroadcast> cli = null;

	private transient RPCDispatcher rpc;

	private String name;

	private SplitFunction func;

	private PeerFilter filter;

	private HashMap<String, Object> defaultValue = new HashMap<>();

	// private TLongHashSet vertices = new TLongHashSet();

	// private TLongHashSet disabled = new TLongHashSet();

	private int minNodes;

	public InMemoryGraph(RPCDispatcher rpc, String name, SplitFunction func,
			PeerFilter filter, int minNodes) throws Exception {
		this.rpc = rpc;
		this.name = name;
		this.func = func;
		this.filter = filter;
		this.minNodes = minNodes;
		createClient();
	}

	private Graph getGraph(Long o) throws Exception {
		if (cli == null) {
			createClient();
		}
		Graph graph = cli.get(func.getPeer(o, cli.getPeers()));
		return graph;
	}

	private void createClient() throws Exception {
		cli = rpc.manage(new GraphFactory(rpc, name), filter, rpc.getCluster()
				.getLocalPeer());
		cli.waitForClient(minNodes);
		for (Peer p : cli.getPeers()) {
			rpc.registerIfAbsent(p, name, new PregelGraphLocal(name, true));
		}

	}

	@Override
	public void putOutgoing(Long from, Long to) throws Exception {
		addVertex(from);
		addVertex(to);
		createVertex(to);
		getGraph(from).putOutgoing(from, to);
	}

	@Override
	public void putIncoming(Long to, Long from) throws Exception {
		addVertex(to);
		addVertex(from);
		createVertex(from);
		getGraph(to).putIncoming(to, from);
	}

	private void addVertex(Long to) {
		// synchronized (vertices) {
		// vertices.add(to);
		// }
	}

	@Override
	public void setVal(Long v, String k, Object val) throws Exception {
		addVertex(v);
		getGraph(v).setVal(v, k, val);
	}

	@Override
	public void removeOutgoing(Long from, Long to) throws Exception {
		getGraph(from).removeOutgoing(from, to);
	}

	@Override
	public Object get(Long v, String k) throws Exception {
		Object ret = getGraph(v).get(v, k);
		if (ret == null)
			ret = getDefaultValue(k);
		return ret;
	}

	@Override
	public int vertexSize() throws Exception {
		// return vertexCount;
		// synchronized (vertices) {
		// return vertices.size();
		// }
		int sum = 0;
		for (Graph g : cli.getAll()) {
			sum += g.vertexSize();
		}
		return sum;
	}

	@Override
	public Collection<Long> vertices() throws Exception {
		// return new TLongSetDecorator(vertices);
		Iterator<Graph> graphIt = cli.getAll().iterator();
		return new AbstractCollection<Long>() {
			@Override
			public Iterator<Long> iterator() {
				return new Iterator<Long>() {
					Iterator<Long> it;

					@Override
					public Long next() {
						return it.next();
					}

					@Override
					public boolean hasNext() {
						if (it != null && it.hasNext())
							return true;
						else
							while (graphIt.hasNext()) {
								try {
									it = graphIt.next().vertices().iterator();
								} catch (Exception e) {
									e.printStackTrace();
								}
								if (it.hasNext())
									return true;
							}
						return false;
					}
				};
			}

			@Override
			public int size() {
				try {
					return vertexSize();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return 0;
			}
		};
	}

	@Override
	public int getAdyacencySize(Long v) throws Exception {
		return getGraph(v).getAdyacencySize(v);
	}

	@Override
	public Collection<Long> getOutgoing(Long vertex) throws Exception {
		return getGraph(vertex).getOutgoing(vertex);
	}

	@Override
	public Collection<Long> getIncoming(Long v) throws Exception {
		return getGraph(v).getIncoming(v);
	}

	@Override
	public void setDefaultValue(String k, Object d) throws Exception {
		defaultValue.put(k, d);
	}

	@Override
	public Object getDefaultValue(String k) throws Exception {
		return defaultValue.get(k);
	}

	@Override
	public int getOutgoingSize(Long v) throws Exception {
		return getGraph(v).getOutgoingSize(v);
	}

	@Override
	public void disable(Long v) throws Exception {
		// synchronized (vertices) {
		// vertices.remove(v);
		// }
		// synchronized (disabled) {
		// disabled.add(v);
		//
		// }
		getGraph(v).disable(v);
	}

	@Override
	public void enableAll() throws Exception {
		// synchronized (vertices) {
		// vertices.addAll(disabled);
		// }
		// synchronized (disabled) {
		// disabled.clear();
		// }
		for (Graph g : cli.getAll()) {
			g.enableAll();
		}
	}

	@Override
	public void putLink(Long o, Long dest) throws Exception {
		putOutgoing(o, dest);
		putIncoming(dest, o);
	}

	public String print() throws Exception {
		StringBuilder ret = new StringBuilder();
		for (Peer p : cli.getPeers()) {
			ret.append("On worker : " + p + "\n");
			ret.append(cli.get(p).print() + "\n");
		}
		return ret.toString();

	}

	@Override
	public void disableLink(Long v, Long from) throws Exception {
		disableOutgoing(v, from);
		disableIncoming(from, v);

	}

	@Override
	public void disableOutgoing(Long v, Long from) throws Exception {
		getGraph(v).disableOutgoing(v, from);
	}

	@Override
	public void disableIncoming(Long from, Long v) throws Exception {
		getGraph(from).disableIncoming(from, v);
	}

	@Override
	public String getName() throws Exception {
		return name;
	}

	@Override
	public boolean createVertex(Long from) throws Exception {
		return getGraph(from).createVertex(from);

	}

	@Override
	public void setRPC(RPCDispatcher rpc) {
		this.rpc = rpc;
	}

	private transient ExecutorService pool;

	private transient Semaphore max;

	public void putOutgoing(List<Long[]> cache) throws Exception {

		HashMap<Graph, Set<Long>> create = new HashMap<>();
		HashMap<Graph, List<Long[]>> div = new HashMap<>();
		for (Long[] e : cache) {
			Graph g = getGraph(e[0]);
			List<Long[]> l = div.get(g);
			if (l == null) {
				l = new ArrayList<Long[]>();
				div.put(g, l);
			}
			l.add(e);

			g = getGraph(e[1]);
			Set<Long> toCreate = create.get(g);
			if (toCreate == null) {
				toCreate = new HashSet<Long>();
				create.put(g, toCreate);
			}
			toCreate.add(e[1]);
		}

		for (Entry<Graph, List<Long[]>> e : div.entrySet()) {
			max.acquire();
			pool.execute(new Runnable() {
				@Override
				public void run() {
					try {
						e.getKey().putOutgoing(e.getValue());
						for (Long[] longs : e.getValue()) {
							createVertex(longs[1]);
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
					max.release();
				}
			});
		}

		for (Entry<Graph, Set<Long>> e : create.entrySet()) {
			max.acquire();
			pool.execute(new Runnable() {
				@Override
				public void run() {
					try {
						e.getKey().createVertices(e.getValue());
					} catch (Exception e) {
						e.printStackTrace();
					}
					max.release();
				}
			});
		}

	}

	@Override
	public void createVertices(Set<Long> value) {
	}

	public void load(String pathname) throws Exception {

		pool = Executors.newFixedThreadPool(5);
		max = new Semaphore(10);
		BufferedReader s = new BufferedReader(
				new FileReader(new File(pathname)));

		List<Long[]> cache = new ArrayList<>();

		int cont = 0;
		while (s.ready()) {
			if (cont++ % 1000 == 0)
				System.out.println("Cont: " + cont);

			String[] rel = s.readLine().replaceAll("\\s", " ").trim()
					.split(" ");
			Long from = Long.valueOf(rel[0]);
			Long to = Long.valueOf(rel[1]);

			if (cache.size() == LIMIT) {
				putOutgoing(cache);
				cache.clear();
			}
			cache.add(new Long[] { from, to });
		}
		s.close();

		if (!cache.isEmpty()) {
			putOutgoing(cache);
			cache.clear();
		}

		pool.shutdown();
		pool.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

		pool = new ForkJoinPool();
	}
}