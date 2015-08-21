package edu.jlime.graphly.rec;

import edu.jlime.pregel.coordinator.Aggregator;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.set.hash.TLongHashSet;

public class IntersectAggregator implements SetAggregator {
	TLongHashSet set = new TLongHashSet();

	@Override
	public void superstep(int s) {
	}

	@Override
	public Aggregator copy() {
		return new IntersectAggregator();
	}

	@Override
	public void reset() {
	}

	@Override
	public void merge(Aggregator value) {
		add(((IntersectAggregator) value).set);
	}

	public synchronized void add(TLongHashSet out) {
		if (set.isEmpty())
			set.addAll(out);
		else {
			TLongIterator it = set.iterator();
			while (it.hasNext()) {
				if (!out.contains(it.next()))
					it.remove();
			}
		}
	}

	public TLongHashSet getSet() {
		return set;
	}

	@Override
	public float get() {
		return set.size();
	}
}
