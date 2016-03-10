package edu.jlime.graphly.rec;

import edu.jlime.pregel.coordinator.Aggregator;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.set.hash.TLongHashSet;

public class JaccardAggregator implements SetAggregator {
	TLongHashSet inter = null;
	TLongHashSet union = new TLongHashSet();

	@Override
	public void superstep(int s) {
	}

	@Override
	public Aggregator copy() {
		return new JaccardAggregator();
	}

	@Override
	public void reset() {
	}

	@Override
	public void merge(Aggregator value) {
		if (inter != null)
			addInter(((JaccardAggregator) value).inter);
		addUnion(((JaccardAggregator) value).union);
	}

	private synchronized void addUnion(TLongHashSet v) {
		union.addAll(v);
	}

	private synchronized void addInter(TLongHashSet v) {
		if (inter == null)
			inter = new TLongHashSet(v);
		else {
			TLongIterator it = inter.iterator();
			while (it.hasNext()) {
				if (!v.contains(it.next()))
					it.remove();
			}
		}
	}

	@Override
	public float get() {
		if (inter == null)
			return 0f;
		return inter.size() / (float) union.size();
	}

	@Override
	public void add(TLongHashSet v) {
		addInter(v);
		addUnion(v);
	}

	@Override
	public TLongHashSet getSet() {
		// Makes no sense for jaccard
		return null;
	}

}
