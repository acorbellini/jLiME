package edu.jlime.graphly.rec;

import edu.jlime.pregel.coordinator.Aggregator;
import gnu.trove.set.hash.TLongHashSet;

public interface SetAggregator extends Aggregator {
	public void add(TLongHashSet v);

	public TLongHashSet getSet();
}
