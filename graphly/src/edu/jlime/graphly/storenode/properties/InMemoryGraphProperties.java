package edu.jlime.graphly.storenode.properties;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import gnu.trove.map.hash.TLongObjectHashMap;

public class InMemoryGraphProperties {
	ConcurrentHashMap<String, Map<String, TLongObjectHashMap<Object>>> props = new ConcurrentHashMap<>();

	public void put(String graph, long vid, String k, Object val) {
		Map<String, TLongObjectHashMap<Object>> map = props.get(graph);
		if (map == null) {
			synchronized (props) {
				map = props.get(graph);
				if (map == null) {
					map = new ConcurrentHashMap<String, TLongObjectHashMap<Object>>();
					props.put(graph, map);
				}
			}
		}
		insertIntoGraphMap(map, vid, k, val);
	}

	private void insertIntoGraphMap(Map<String, TLongObjectHashMap<Object>> props, long vid, String k, Object val) {
		TLongObjectHashMap<Object> map = props.get(k);
		if (map == null) {
			synchronized (props) {
				map = props.get(k);
				if (map == null) {
					map = new TLongObjectHashMap<>();
					props.put(k, map);
				}
			}
		}

		synchronized (map) {
			map.put(vid, val);
		}

	}

	public Object get(String graph, long vid, String k) {
		Map<String, TLongObjectHashMap<Object>> m = props.get(graph);
		if (m == null)
			return null;
		TLongObjectHashMap<Object> sub = m.get(k);
		if (sub == null)
			return null;
		synchronized (sub) {
			return sub.get(vid);
		}
	}

	@Override
	public String toString() {
		return props.toString();
	}
}