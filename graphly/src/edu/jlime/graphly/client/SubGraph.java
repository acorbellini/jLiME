package edu.jlime.graphly.client;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import edu.jlime.collections.adjacencygraph.get.Dir;
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

	Cache<SubEdge, long[]> edgecache = CacheBuilder.newBuilder()
			.maximumSize(10000).build();

	Cache<SubEdge, Integer> countcache = CacheBuilder.newBuilder()
			.maximumSize(100000).build();

	public SubGraph(Graphly graphly, long[] all) {
		this.g = graphly;
		this.vertices = all;
	}

	public long[] getEdges(Dir in, Long vid) throws ExecutionException {
		return edgecache.get(new SubEdge(in, vid), new Callable<long[]>() {

			@Override
			public long[] call() throws Exception {
				long[] edges = null;
				try {
					edges = g.getEdges(in, vid);
					if (edges == null)
						return new long[] {};
					else
						return GraphlyUtil.filter(edges, vertices);
				} catch (Exception e) {
					e.printStackTrace();
				}
				return new long[] {};
			}

		});
	}

	public int getEdgesCount(Dir in, long vid) throws ExecutionException {
		return countcache.get(new SubEdge(in, vid), new Callable<Integer>() {

			@Override
			public Integer call() throws Exception {
				return g.getEdgesCount(in, vid, vertices);
			}

		});
	}

	public Long getRandomEdge(Dir dir, Long curr) throws ExecutionException {
		long[] edges = getEdges(dir, curr);
		if (edges == null || edges.length == 0)
			return null;
		return edges[(int) (Math.random() * edges.length)];
	}

}
