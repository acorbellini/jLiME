package edu.jlime.metrics.metric;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class MetricList implements Iterable<MetricListItem> {

	IMetrics metrics;

	Set<String> keys = new HashSet<>();

	private String root;

	public MetricList(String k, IMetrics metrics) {
		this.root = k;
		this.metrics = metrics;
	}

	@Override
	public Iterator<MetricListItem> iterator() {
		final Iterator<String> it = keys.iterator();
		return new Iterator<MetricListItem>() {

			@Override
			public boolean hasNext() {
				return it.hasNext();
			}

			@Override
			public MetricListItem next() {
				return new MetricListItem(it.next(), metrics);
			}

			@Override
			public void remove() {
			}
		};
	}

	public void add(String el) {
		keys.add(el);
	}

	public MetricListItem findFirst(String prefix) {
		for (String k : keys) {
			String val = k;
			if (k.contains("."))
				val = k.substring(k.lastIndexOf(".") + 1, k.length());
			String[] split = prefix.split("|");
			for (String pre : split) {
				if (val.startsWith(pre))
					return new MetricListItem(k, metrics);
			}

		}
		return null;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		for (MetricListItem ml : this) {
			builder.append(ml.root + "\n");
		}
		return builder.toString();
	}
}