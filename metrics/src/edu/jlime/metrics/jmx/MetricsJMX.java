package edu.jlime.metrics.jmx;

import java.lang.management.ManagementFactory;
import java.util.Map.Entry;
import java.util.SortedMap;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import edu.jlime.metrics.metric.Metric;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.metrics.metric.MetricsListener;

public class MetricsJMX {

	private Metrics mgr;

	public MetricsJMX(Metrics mgr) {
		this.mgr = mgr;
	}

	protected void removeMetric(String k, Metric<?> metric) {
		MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
		try {
			mbs.unregisterMBean(getObjectName(k));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private ObjectName getObjectName(String k)
			throws MalformedObjectNameException {
		String name = getName(k);
		String type = getType(k);
		return new ObjectName("jlime.metrics." + mgr.getId() + ":"
				+ (type != null ? "type=" + type + "," : "") + "name=" + name);
	}

	private String getType(String k) {
		if (!k.contains("."))
			return null;
		return k.substring(0, k.lastIndexOf("."));
	}

	private String getName(String k) {
		if (!k.contains("."))
			return k;
		return k.substring(k.lastIndexOf(".") + 1, k.length());
	}

	private void addMetric(String k, Metric<?> metric) {
		MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
		try {
			mbs.registerMBean(metric, getObjectName(k));
		} catch (Exception e) {
		}
	}

	public void start() {
		SortedMap<String, Metric<?>> list = mgr.getMetrics();

		for (Entry<String, Metric<?>> metric : list.entrySet())
			addMetric(metric.getKey(), metric.getValue());

		mgr.listen(new MetricsListener() {

			@Override
			public void metricDeleted(String k, Metric<?> m) {
				removeMetric(k, m);
			}

			@Override
			public void metricAdded(String k, Metric<?> m) {
				addMetric(k, m);
			}
		});
	}
}
