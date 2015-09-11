package edu.jlime.graphly.traversal;

import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;

public class RandomStep implements Step {

	private Traversal tr;
	private Dir dir;
	private long[] subset;

	public RandomStep(Dir dir, long[] subset, Traversal tr) {
		this.dir = dir;
		this.tr = tr;
		this.subset = subset;
	}

	@Override
	public TraversalResult exec(TraversalResult before) throws Exception {
		TLongIterator it = before.vertices().iterator();
		TLongArrayList ret = new TLongArrayList();
		while (it.hasNext()) {
			long v = it.next();
			Long r = tr.getGraph().getRandomEdge(v, subset, dir);
			if (r != null)
				ret.add(r);
		}
		return new VertexResult(ret.toArray());
	}
}
