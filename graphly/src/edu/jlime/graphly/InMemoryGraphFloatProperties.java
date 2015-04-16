package edu.jlime.graphly;

import gnu.trove.map.hash.TLongFloatHashMap;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryGraphFloatProperties {
	public static final Float NOT_FOUND = Float.MIN_VALUE;
	ConcurrentHashMap<String, Map<String, TLongFloatHashMap>> props = new ConcurrentHashMap<>();

	public void put(String graph, long vid, String k, float val) {
		Map<String, TLongFloatHashMap> map = props.get(graph);
		if (map == null) {
			synchronized (props) {
				map = props.get(graph);
				if (map == null) {
					map = new ConcurrentHashMap<String, TLongFloatHashMap>();
					props.put(graph, map);
				}
			}
		}
		insertIntoGraphMap(map, vid, k, val);
	}

	private void insertIntoGraphMap(Map<String, TLongFloatHashMap> props,
			long vid, String k, float val) {
		TLongFloatHashMap map = props.get(k);
		if (map == null) {
			synchronized (props) {
				map = props.get(k);
				if (map == null) {
					map = new TLongFloatHashMap();
					props.put(k, map);
				}
			}
		}

		synchronized (map) {
			map.put(vid, val);
		}

	}

	public float get(String graph, long vid, String k) {
		Map<String, TLongFloatHashMap> m = props.get(graph);
		if (m == null)
			return Float.MIN_VALUE;
		TLongFloatHashMap sub = m.get(k);
		if (sub == null)
			return Float.MIN_VALUE;
		synchronized (sub) {
			return sub.get(vid);
		}
	}

	@Override
	public String toString() {
		return props.toString();
	}
}
