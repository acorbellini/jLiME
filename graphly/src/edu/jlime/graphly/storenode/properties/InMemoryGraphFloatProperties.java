package edu.jlime.graphly.storenode.properties;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import gnu.trove.iterator.TLongFloatIterator;
import gnu.trove.map.hash.TLongFloatHashMap;

public class InMemoryGraphFloatProperties {
	private static final long KEY_NOT_FOUND = Long.MIN_VALUE;
	public static final Float VALUE_NOT_FOUND = Float.MIN_VALUE;
	ConcurrentHashMap<String, Map<String, TLongFloatHashMap>> props = new ConcurrentHashMap<>();

	public void put(String graph, long vid, String k, float val) {
		TLongFloatHashMap map = getGraphProps(graph, k);
		synchronized (map) {
			map.put(vid, val);
		}
	}

	private TLongFloatHashMap getGraphProps(String graph, String k) {
		Map<String, TLongFloatHashMap> gmap = props.get(graph);
		if (gmap == null) {
			synchronized (props) {
				gmap = props.get(graph);
				if (gmap == null) {
					gmap = new ConcurrentHashMap<String, TLongFloatHashMap>();
					props.put(graph, gmap);
				}
			}
		}

		TLongFloatHashMap map = gmap.get(k);
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

	public float get(String graph, long vid, String k) {
		Map<String, TLongFloatHashMap> m = props.get(graph);
		if (m == null)
			return VALUE_NOT_FOUND;
		TLongFloatHashMap sub = m.get(k);
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

	public void putAll(String graph, String k, TLongFloatHashMap subProp) {
		TLongFloatHashMap map = getGraphProps(graph, k);
		synchronized (map) {
			map.putAll(subProp);
		}
	}

	public void addAll(String graph, String k, TLongFloatHashMap subProp) {
		TLongFloatHashMap map = getGraphProps(graph, k);
		synchronized (map) {
			TLongFloatIterator it = subProp.iterator();
			while (it.hasNext()) {
				it.advance();
				map.adjustOrPutValue(it.key(), it.value(), it.value());
			}

		}
	}

	public TLongFloatHashMap getAll(String graph, String string) {
		return getGraphProps(graph, string);
	}

	public void removeAll(String graph, String string) {
		TLongFloatHashMap map = getGraphProps(graph, string);
		synchronized (map) {
			map.clear();
		}

	}
}
