package edu.jlime.core.cache;

import org.apache.commons.collections.map.LRUMap;

public class Cache<T, R> {

	private LRUMap cache;

	public Cache(int size) {
		// cache = new LRUMap<>(size);
		cache = new LRUMap(size);
	}

	public synchronized R get(T t) {
		return (R) cache.get(t);
	}

	public synchronized void put(T t, R r) {
		cache.put(t, r);
	}
}
