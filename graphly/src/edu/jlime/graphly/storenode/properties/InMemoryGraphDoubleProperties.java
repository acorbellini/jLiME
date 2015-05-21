package edu.jlime.graphly.storenode.properties;

import gnu.trove.map.hash.TLongDoubleHashMap;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryGraphDoubleProperties {
	public static final double NOT_FOUND = Double.MIN_VALUE;
	ConcurrentHashMap<String, Map<String, TLongDoubleHashMap>> props = new ConcurrentHashMap<>();

	public void put(String graph, long vid, String k, double val) {
		Map<String, TLongDoubleHashMap> map = props.get(graph);
		if (map == null) {
			synchronized (props) {
				map = props.get(graph);
				if (map == null) {
					map = new ConcurrentHashMap<String, TLongDoubleHashMap>();
					props.put(graph, map);
				}
			}
		}
		insertIntoGraphMap(map, vid, k, val);
	}

	private void insertIntoGraphMap(Map<String, TLongDoubleHashMap> props,
			long vid, String k, double val) {
		TLongDoubleHashMap map = props.get(k);
		if (map == null) {
			synchronized (props) {
				map = props.get(k);
				if (map == null) {
					map = new TLongDoubleHashMap();
					props.put(k, map);
				}
			}
		}

		synchronized (map) {
			map.put(vid, val);
		}

	}

	public double get(String graph, long vid, String k) {
		Map<String, TLongDoubleHashMap> m = props.get(graph);
		if (m == null)
			return Double.MIN_VALUE;
		TLongDoubleHashMap sub = m.get(k);
		if (sub == null)
			return Double.MIN_VALUE;
		synchronized (sub) {
			return sub.get(vid);
		}
	}

	@Override
	public String toString() {
		return props.toString();
	}
}
