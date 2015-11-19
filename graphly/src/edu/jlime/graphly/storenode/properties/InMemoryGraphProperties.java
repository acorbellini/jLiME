package edu.jlime.graphly.storenode.properties;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;

public class InMemoryGraphProperties implements Serializable {
	ConcurrentHashMap<String, Map<String, TLongObjectMap<Object>>> props = new ConcurrentHashMap<>();

	public void put(String graph, long vid, String k, Object val) {
		TLongObjectMap<Object> map = getGraphProperties(graph, k);
		synchronized (map) {
			map.put(vid, val);
		}
	}

	private TLongObjectMap<Object> getGraphProperties(String graph, String k) {
		Map<String, TLongObjectMap<Object>> graph_map = getGraphMap(graph);

		TLongObjectMap<Object> map = graph_map.get(k);
		if (map == null) {
			synchronized (graph_map) {
				map = graph_map.get(k);
				if (map == null) {
					map = new TLongObjectHashMap<>();
					graph_map.put(k, map);
				}
			}
		}

		return map;
	}

	private Map<String, TLongObjectMap<Object>> getGraphMap(String graph) {
		Map<String, TLongObjectMap<Object>> graph_map = props.get(graph);
		if (graph_map == null) {
			synchronized (props) {
				graph_map = props.get(graph);
				if (graph_map == null) {
					graph_map = new ConcurrentHashMap<String, TLongObjectMap<Object>>();
					props.put(graph, graph_map);
				}
			}
		}
		return graph_map;
	}

	public Object get(String graph, long vid, String k) {
		Map<String, TLongObjectMap<Object>> m = props.get(graph);
		if (m == null)
			return null;
		TLongObjectMap<Object> sub = m.get(k);
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

	public Set<String> getProperties() {
		HashSet<String> ret = new HashSet<>();
		for (Entry<String, Map<String, TLongObjectMap<Object>>> e : props
				.entrySet()) {
			for (Entry<String, TLongObjectMap<Object>> e2 : e.getValue()
					.entrySet()) {
				ret.add(e2.getKey());
			}
		}
		return ret;
	}

	public TLongObjectMap<Object> get(String graph, String k) {
		Map<String, TLongObjectMap<Object>> m = props.get(graph);
		if (m == null)
			return null;
		return m.get(k);
	}

	public synchronized void putAll(String graph, String key,
			TLongObjectMap<Object> value) {
		TLongObjectMap<Object> p_key = getGraphProperties(graph, key);
		synchronized (p_key) {
			p_key.putAll(value);
		}
	}
}