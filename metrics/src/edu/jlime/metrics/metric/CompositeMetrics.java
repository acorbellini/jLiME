package edu.jlime.metrics.metric;

import java.util.HashSet;
import java.util.Map;

public class CompositeMetrics<T> {

	private Map<T, Metrics> map;

	public CompositeMetrics(Map<T, Metrics> map) {
		this.map = map;
	}

	public Map<T, Metrics> getMap() {
		return map;
	}

	public HashSet<T> getKeys() {
		return new HashSet<>(map.keySet());
	}

	public boolean contains(T peer) {
		return map.containsKey(peer);
	}

	public Metrics get(T peer) {
		return map.get(peer);
	}
}
