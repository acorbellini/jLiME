package edu.jlime.core.lrumap;

import edu.jlime.core.cache.jLiMELRUMap;

public class LRUMapTest {

	public static void main(String[] args) {
		jLiMELRUMap<String, String> lru = new jLiMELRUMap<>(2);

		lru.put("a", "b");
		lru.put("b", "y");
		lru.put("c", "b");
		lru.put("a", "x");
		lru.put("e", "b");
		lru.put("f", "z");

		System.out.println(lru.get("a"));
		System.out.println(lru.get("e"));

	}
}
