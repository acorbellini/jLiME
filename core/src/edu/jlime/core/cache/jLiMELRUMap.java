package edu.jlime.core.cache;

import java.util.LinkedHashMap;
import java.util.Map;

public class jLiMELRUMap<K, V> {

	private final Map<K, V> cacheMap;

	public jLiMELRUMap(final int cacheSize) {
		this.cacheMap = new LinkedHashMap<K, V>(cacheSize, 0.75f, true) {
			@Override
			protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
				return size() > cacheSize;
			}
		};
	}

	public synchronized void put(K key, V elem) {
		cacheMap.put(key, elem);
	}

	public synchronized V get(K key) {
		return cacheMap.get(key);
	}

	public synchronized V atomicGetAndSet(K key, V elem) {
		V result = get(key);
		put(key, elem);
		return result;
	}
}