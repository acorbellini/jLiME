package edu.jlime.graphly.rec;

import edu.jlime.pregel.coordinator.Aggregator;
import gnu.trove.map.hash.TLongFloatHashMap;

public class CountAggregator implements Aggregator {

	private TLongFloatHashMap hash;

	@Override
	public void superstep(int s) {
		reset();
	}

	@Override
	public Aggregator copy() {
		return new CountAggregator();
	}

	@Override
	public void reset() {
		hash.clear();
	}

	@Override
	public void merge(Aggregator value) {
		CountAggregator other = ((CountAggregator) value);
		this.hash.putAll(other.hash);

	}

	public synchronized void set(long v, float sum) {
		hash.put(v, sum);
	}

	@Override
	public float get() {
		return 0f;
	}

}
