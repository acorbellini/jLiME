package edu.jlime.core.marshalling.converters;

import java.util.ArrayList;
import java.util.TreeMap;
import java.util.Map.Entry;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.marshalling.TypeConverter;
import edu.jlime.core.marshalling.TypeConverters;
import edu.jlime.metrics.metric.Metric;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.util.ByteBuffer;

public class MetricConverter implements TypeConverter {

	private TypeConverters tc;

	public MetricConverter(TypeConverters typeConverters) {
		this.tc = typeConverters;
	}

	@Override
	public void toArray(Object o, ByteBuffer buffer, Peer client)
			throws Exception {
		Metrics m = (Metrics) o;
		ArrayList<Entry<String, Metric<?>>> entries = new ArrayList<>(m
				.getMetrics().entrySet());
		buffer.putInt(entries.size());
		for (Entry<String, Metric<?>> entry : entries) {
			buffer.putString(entry.getKey());
			tc.objectToByteArray(entry.getValue(), buffer, client);
		}

	}

	@Override
	public Object fromArray(ByteBuffer buff) throws Exception {

		TreeMap<String, Metric<?>> map = new TreeMap<>();
		int size = buff.getInt();
		for (int i = 0; i < size; i++) {
			map.put(buff.getString(), (Metric<?>) tc.getObjectFromArray(buff));
		}
		Metrics m = new Metrics(map);
		return m;
	}
}
