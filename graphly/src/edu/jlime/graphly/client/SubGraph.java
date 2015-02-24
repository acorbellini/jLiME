package edu.jlime.graphly.client;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.util.GraphlyUtil;

public class SubGraph {

	private static class SubEdge {
		Dir dir;
		long vid;

		public SubEdge(Dir dir, long vid) {
			super();
			this.dir = dir;
			this.vid = vid;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((dir == null) ? 0 : dir.hashCode());
			result = prime * result + (int) (vid ^ (vid >>> 32));
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			SubEdge other = (SubEdge) obj;
			if (dir != other.dir)
				return false;
			if (vid != other.vid)
				return false;
			return true;
		}

	}

	private Graphly g;
	private long[] vertices;

	Cache<Long, Object> propcache = CacheBuilder.newBuilder().build();

	Cache<SubEdge, long[]> edgecache = CacheBuilder.newBuilder().build();

	Map<SubEdge, Integer> countcache = new ConcurrentHashMap<>();

	public SubGraph(Graphly graphly, long[] all) {
		this.g = graphly;
		this.vertices = all;
		Arrays.sort(vertices);
	}

	public long[] getEdges(final Dir in, final Long vid)
			throws ExecutionException {
		return edgecache.get(new SubEdge(in, vid), new Callable<long[]>() {

			@Override
			public long[] call() throws Exception {
				long[] edges = null;
				try {
					edges = g.getEdges(in, vid);
					if (edges == null)
						return new long[] {};
					else {
						Arrays.sort(edges);
						return GraphlyUtil.filter(edges, vertices);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				return new long[] {};
			}

		});
	}

	public int getEdgesCount(Dir in, long vid) throws Exception {
		SubEdge subEdge = new SubEdge(in, vid);
		Integer c = countcache.get(subEdge);
		if (c == null) {
			synchronized (countcache) {
				if (c == null) {
					c = g.getEdgesCount(in, vid, vertices);
					countcache.put(subEdge, c);
				}
			}
		}
		return c;
	}

	public long getRandomEdge(Dir dir, Long curr) throws ExecutionException {
		long[] edges = getEdges(dir, curr);
		if (edges == null || edges.length == 0)
			return -1;
		return edges[(int) (Math.random() * edges.length)];
	}

	public void invalidateProperties() {
		propcache.invalidateAll();
	}

	public Object getProperty(final Long w, final String a, final Object f)
			throws ExecutionException {
		return propcache.get(w, new Callable<Object>() {

			@Override
			public Object call() throws Exception {
				return g.getProperty(w, a, f);
			}
		});
	}

}
