package edu.jlime.metrics.metric;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;

import edu.jlime.metrics.meters.Accumulator;
import edu.jlime.metrics.meters.Counter;
import edu.jlime.metrics.meters.Gauge;
import edu.jlime.metrics.meters.Meter;
import edu.jlime.metrics.meters.MetricSet;
import edu.jlime.metrics.meters.Simple;

public class Metrics implements Serializable, IMetrics {

	private static final long serialVersionUID = -4992991694532916282L;

	// This ones are fixed.

	private SortedMap<String, Metric<?>> metrics = Collections
			.synchronizedSortedMap(new TreeMap<String, Metric<?>>());

	private transient Timer timer = new Timer(true);

	private transient ArrayList<MetricsListener> listeners = new ArrayList<>();

	private static final long FREQ = 5000;

	public Metrics() {
	}

	public Metrics(TreeMap<String, Metric<?>> map) {
		metrics.putAll(map);
	}

	public void stop() {
		timer.cancel();
	}

	private Metric<?> create(String k, MetricFactory<?> meterFactory) {
		Metric<?> g = get(k);
		if (g == null) {
			synchronized (metrics) {
				g = get(k);
				if (g == null) {
					g = meterFactory.create();
					put(k, g);
				}
			}
		}
		return g;
	}

	private void put(String k, Metric<?> m) {
		metrics.put(k, m);
		notifyMetricAdded(k, m);
	}

	private void notifyMetricAdded(String k, Metric<?> m) {
		for (MetricsListener metricsListener : listeners) {
			metricsListener.metricAdded(k, m);
		}
	}

	public void listen(MetricsListener m) {
		listeners.add(m);
	}

	public void removeListener(MetricsListener m) {
		listeners.remove(m);
	}

	@Override
	public Metric<?> get(String k) {
		return metrics.get(k);
	}

	@Override
	public MetricList list(String k) {
		MetricList ret = new MetricList(k, this);

		Map<String, Metric<?>> m = getAll(k);
		Iterator<Entry<String, Metric<?>>> it = m.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, Metric<?>> e = it.next();
			if (e.getKey().startsWith(k))
				ret.add(e.getKey().substring(0,
						e.getKey().indexOf(".", k.length() + 1)));
			else
				return ret;
		}
		return ret;
	}

	public Map<String, Metric<?>> getAll(String k) {
		String root = "";
		String el = k;
		if (k.contains(".")) {
			root = k.substring(0, k.lastIndexOf(".") + 1);
			el = k.substring(k.lastIndexOf(".") + 1, k.length());
		}

		String nextLetter = String.valueOf((char) (el.charAt(0) + 1));
		synchronized (metrics) {
			SortedMap<String, Metric<?>> m = new TreeMap<>(metrics.subMap(k,
					root + nextLetter));
			return m;
		}
	}

	public void createTimedSensor(final SensorMeasure timed) {
		timer.schedule(new TimerTask() {
			@Override
			public void run() {
				try {
					timed.proc(Metrics.this);
				} catch (Exception e) {
					e.printStackTrace();
				}

			}
		}, 0, FREQ);
	}

	public static String format(Object... vals) {
		StringBuilder builder = new StringBuilder();
		boolean first = true;
		for (Object object : vals) {
			if (!first)
				builder.append(",");
			else
				first = false;
			builder.append(object.toString());
		}
		return builder.toString();
	}

	public void deleteAll(String k) {
		synchronized (metrics) {
			SortedMap<String, Metric<?>> m = metrics.tailMap(k);
			Iterator<Entry<String, Metric<?>>> it = m.entrySet().iterator();
			while (it.hasNext()) {
				Entry<String, Metric<?>> e = it.next();
				if (e.getKey().startsWith(k))
					it.remove();
				else
					return;
			}
		}
	}

	@Override
	public Meter meter(String k) {
		return (Meter) create(k, MetricFactory.meterFactory);
	}

	@Override
	public Simple simple(String k) {
		return (Simple) create(k, MetricFactory.simpleFactory);
	}

	@Override
	public Gauge gauge(String k) {
		return (Gauge) create(k, MetricFactory.gaugeFactory);
	}

	@Override
	public Accumulator accumulator(String k) {
		return (Accumulator) create(k, MetricFactory.accumulatorFactory);
	}

	@Override
	public void simple(String k, Object v) {
		simple(k).update(v);
	}

	@Override
	public Counter counter(String k) {
		return (Counter) create(k, MetricFactory.counterFactory);
	}

	public MetricSet set(String k) {
		return (MetricSet) create(k, MetricFactory.setFactory);
	}

	public SortedMap<String, Metric<?>> getMetrics() {
		synchronized (metrics) {
			return new TreeMap<>(metrics);
		}

	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Update every " + FREQ + ": \n");
		synchronized (metrics) {
			for (Entry<String, Metric<?>> e : metrics.entrySet())
				builder.append(e.getKey() + "-" + e.getValue() + "\n");
		}

		return builder.toString();
	}

	public void gauge(String string, float f) {
		this.gauge(string).update(f);
	}
}
