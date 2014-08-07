package edu.jlime.metrics.metric;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MetricList implements Iterable<MetricListItem> {

	IMetrics metrics;

	List<String> keys = new ArrayList<>();

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
			if (k.startsWith(prefix))
				return new MetricListItem(k, metrics);
		}
		return null;
	}
}