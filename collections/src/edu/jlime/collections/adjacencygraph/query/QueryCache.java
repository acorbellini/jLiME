package edu.jlime.collections.adjacencygraph.query;

import edu.jlime.core.cache.Cache;

public class QueryCache {

	private static final int QUERY_CACHE_SIZE = 100;

	private static Cache<Query<?>, Object> cache = new Cache<>(QUERY_CACHE_SIZE);

	public static <R> void put(Query<R> q, Object o) {
		cache.put(q, o);
	}

	public static <R> R get(Query<R> q) {
		return (R) cache.get(q);

	}
}
