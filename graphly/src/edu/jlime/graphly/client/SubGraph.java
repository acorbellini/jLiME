package edu.jlime.graphly.client;

import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.util.GraphlyUtil;

public class SubGraph {

	private static final int LOCKS = 1021;
	private GraphlyGraph g;
	private long[] vertices;

	private ConcurrentHashMap<Long, Map<String, Object>> props = new ConcurrentHashMap<>(
			1000, 0.9f, 8);
	private Object[] propLocks = new Object[LOCKS];

	private ConcurrentHashMap<String, Object> temps = new ConcurrentHashMap<>(
			1000, 0.9f, 8);
	private Object[] tempsLocks = new Object[LOCKS];

	private ConcurrentHashMap<Long, long[]> edges = new ConcurrentHashMap<>(
			1000, 0.9f, 8);
	private Object[] edgesLocks = new Object[LOCKS];

	private ConcurrentHashMap<Long, Integer> counts = new ConcurrentHashMap<>(
			1000, 0.9f, 8);
	private Object[] countsLocks = new Object[LOCKS];

	public SubGraph(GraphlyGraph graphly, long[] all) {
		for (int i = 0; i < LOCKS; i++) {
			propLocks[i] = new Object();
			tempsLocks[i] = new Object();
			edgesLocks[i] = new Object();
			countsLocks[i] = new Object();
		}
		this.g = graphly;
		// long[] vs = all;
		// Arrays.sort(vs);

		vertices = Arrays.copyOf(all, all.length);
		Arrays.sort(vertices);
	}

	public long[] getEdges(final Dir dir, final Long vid)
			throws ExecutionException {
		// if (dir.equals(Dir.IN))
		// return get(dir, edgecacheIn, vid);
		// else if (dir.equals(Dir.OUT))
		// return get(dir, edgecacheOut, vid);
		// return null;
		Long k = vid;
		if (dir.equals(Dir.IN))
			k = -k - 1;

		long[] res = edges.get(k);
		if (res == null) {
			synchronized (edgesLocks[Math.abs((int) (k * 31) % LOCKS)]) {
				res = edges.get(k);
				if (res == null) {

					try {
						long[] e = g.getEdges(dir, vid);
						Arrays.sort(e);
						res = GraphlyUtil.filter(e, vertices);

					} catch (Exception e) {
						e.printStackTrace();
					}
					edges.put(k, res);
				}
			}
		}
		return res;
	}

	public Integer getEdgesCount(final Dir dir, final long vid)
			throws Exception {
		Long k = vid;
		if (dir.equals(Dir.IN))
			k = -k - 1;

		Integer res = counts.get(k);
		if (res == null) {
			synchronized (countsLocks[Math.abs((int) (k * 31) % LOCKS)]) {
				res = counts.get(k);
				if (res == null) {
					res = g.getEdgesCount(dir, vid, vertices);
					counts.put(k, res);
				}
			}
		}
		return res;
	}

	public long getRandomEdge(Dir dir, Long curr) throws ExecutionException {
		long[] edges = getEdges(dir, curr);
		if (edges == null || edges.length == 0)
			return -1;
		return edges[(int) (Math.random() * edges.length)];
	}

	public void invalidateProperties() {
		// propcache.invalidateAll();
		props.clear();
	}

	public Object getProperty(final Long w, final String a, final Object f)
			throws Exception {
		Map<String, Object> p = props.get(w);
		if (p == null) {
			synchronized (propLocks[(int) ((w * 31) % LOCKS)]) {
				p = props.get(w);
				if (p == null) {
					p = new ConcurrentHashMap<>(4, 0.9f, 1);
					props.put(w, p);
				}
			}
		}

		Object ret = p.get(a);
		if (ret == null)
			synchronized (propLocks[(int) ((w * 31) % LOCKS)]) {
				ret = p.get(a);
				if (ret == null) {
					ret = g.getProperty(w, a, f);
					p.put(a, ret);
				}
			}

		return ret;
		// return propcache.get(w, new Callable<Object>() {
		//
		// @Override
		// public Object call() throws Exception {
		// return
		// }
		// });
	}

	public void remove(long vid) {
		// vertices.remove(vid);
	}

	public Object getTemp(String k, Callable<Object> callable) throws Exception {
		Object val = temps.get(k);
		if (val == null) {
			synchronized (tempsLocks[Math.abs((k.hashCode() * 31) % LOCKS)]) {
				val = temps.get(k);
				if (val == null) {
					val = callable.call();
					temps.put(k, val);
				}
			}
		}
		// else
		// System.out.println("FOUND TEMP " + k);
		return val;
	}

	public void invalidateTemps() {
		temps.clear();
	}

	public void loadProperties(String authKey, Object defaultauth)
			throws Exception {
		synchronized (this) {
			Map<Long, Map<String, Object>> props = g.getProperties(vertices,
					authKey);
			for (Entry<Long, Map<String, Object>> e : props.entrySet()) {
				Long l = e.getKey();
				Map<String, Object> value = e.getValue();
				Object object = value.get(authKey);
				if (object == null)
					object = defaultauth;
				setProperty(l, authKey, object);
			}
		}
	}

	private void setProperty(long l, String authKey, Object defaultauth) {
		Map<String, Object> data = props.get(l);
		if (data == null) {
			data = new ConcurrentHashMap<>(4, 0.9f, 1);
			props.put(l, data);
		}
		data.put(authKey, defaultauth);
	}
}
