package edu.jlime.graphly.storenode.properties;

import java.io.Serializable;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import gnu.trove.iterator.TLongFloatIterator;
import gnu.trove.map.TLongFloatMap;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongFloatHashMap;

public class InMemoryGraphFloatProperties implements Serializable {
	private static final long KEY_NOT_FOUND = Long.MIN_VALUE;
	public static final Float VALUE_NOT_FOUND = Float.MIN_VALUE;
	ConcurrentHashMap<String, Map<String, TLongFloatMap>> props = new ConcurrentHashMap<>();

	public void put(String graph, long vid, String k, float val) {
		TLongFloatMap map = getGraphProps(graph, k);
		synchronized (map) {
			map.put(vid, val);
		}
	}

	private TLongFloatMap getGraphProps(String graph, String k) {
		Map<String, TLongFloatMap> gmap = getPropsFor(graph);

		TLongFloatMap map = gmap.get(k);
		if (map == null) {
			synchronized (gmap) {
				map = gmap.get(k);
				if (map == null) {
					map = new TLongFloatHashMap(1000, 0.8f, KEY_NOT_FOUND,
							VALUE_NOT_FOUND);
					gmap.put(k, map);
				}
			}
		}
		return map;
	}

	private Map<String, TLongFloatMap> getPropsFor(String graph) {
		Map<String, TLongFloatMap> gmap = props.get(graph);
		if (gmap == null) {
			synchronized (props) {
				gmap = props.get(graph);
				if (gmap == null) {
					gmap = new ConcurrentHashMap<String, TLongFloatMap>();
					props.put(graph, gmap);
				}
			}
		}
		return gmap;
	}

	public float get(String graph, long vid, String k) {
		Map<String, TLongFloatMap> m = props.get(graph);
		if (m == null)
			return VALUE_NOT_FOUND;
		TLongFloatMap sub = m.get(k);
		if (sub == null)
			return VALUE_NOT_FOUND;
		synchronized (sub) {
			return sub.get(vid);
		}
	}

	@Override
	public String toString() {
		return props.toString();
	}

	public void addAll(String graph, String k, TLongFloatMap subProp) {
		TLongFloatMap map = getGraphProps(graph, k);
		synchronized (map) {
			TLongFloatIterator it = subProp.iterator();
			while (it.hasNext()) {
				it.advance();
				map.adjustOrPutValue(it.key(), it.value(), it.value());
			}

		}
	}

	// public TLongFloatMap getAll(String graph, String string) {
	// return getGraphProps(graph, string);
	// }

	public void removeAll(String graph, String string) {
		TLongFloatMap map = getGraphProps(graph, string);
		synchronized (map) {
			map.clear();
		}

	}

	public void add(String graph, long v, String k, float f) {
		TLongFloatMap map = getGraphProps(graph, k);
		synchronized (map) {
			map.adjustOrPutValue(v, f, f);
		}
	}

	public void putAll(String graph, String key, TLongFloatMap value) {
		// Map<String, TLongFloatMap> g_p = getPropsFor(graph);
		// TLongFloatMap map = g_p.get(key);
		// if (map == null)
		// synchronized (g_p) {
		// map = g_p.get(key);
		// if (map == null) {
		// g_p.put(key, value);
		// return;
		// }
		// }
		TLongFloatMap map = getGraphProps(graph, key);
		synchronized (map) {
			map.putAll(value);
		}
	}

	public Set<String> getProperties() {
		HashSet<String> ret = new HashSet<>();
		for (Entry<String, Map<String, TLongFloatMap>> e : props.entrySet()) {
			for (Entry<String, TLongFloatMap> e2 : e.getValue().entrySet()) {
				ret.add(e2.getKey());
			}
		}
		return ret;
	}

	public TLongFloatMap getAll(String graph, String k) {
		Map<String, TLongFloatMap> m = props.get(graph);
		if (m == null)
			return null;
		TLongFloatMap sub = m.get(k);
		if (sub == null)
			return null;
		synchronized (sub) {
			return new TLongFloatHashMap(sub);
		}

	}
}
