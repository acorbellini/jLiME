package edu.jlime.metrics.metric;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
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

	private TreeMap<String, Metric<?>> metrics = new TreeMap<>();

	private transient Timer timer = new Timer();

	private transient ArrayList<MetricsListener> listeners = new ArrayList<>();

	private static final long FREQ = 3000;

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

	private synchronized void put(String k, Metric<?> m) {
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
	public synchronized Metric<?> get(String k) {
		return metrics.get(k);
	}

	@Override
	public synchronized MetricList list(String k) {
		MetricList ret = new MetricList(k, this);
		SortedMap<String, Metric<?>> m = metrics.tailMap(k);
		Iterator<Entry<String, Metric<?>>> it = m.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, Metric<?>> e = it.next();
			if (e.getKey().startsWith(k))
				ret.add(e.getKey().substring(0,
						e.getKey().indexOf(".", e.getKey().indexOf(k))));
			else
				return ret;
		}
		return ret;
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

	public synchronized void deleteAll(String k) {
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

	public HashMap<String, Metric<?>> getMetrics() {
		return new HashMap<>(metrics);
	}

}
