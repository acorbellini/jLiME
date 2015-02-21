package edu.jlime.graphly.traversal;

import gnu.trove.iterator.TLongIterator;
import gnu.trove.set.hash.TLongHashSet;


public class RandomStep implements Step {

	private GraphlyTraversal tr;
	private Dir dir;
	private long[] subset;

	public RandomStep(Dir dir, long[] subset, GraphlyTraversal tr) {
		this.dir = dir;
		this.tr = tr;
		this.subset = subset;
	}

	@Override
	public TraversalResult exec(TraversalResult before) throws Exception {
		TLongIterator it = before.vertices().iterator();
		TLongHashSet ret = new TLongHashSet();
		while (it.hasNext()) {
			long v = it.next();
			Long r = tr.getGraph().getRandomEdge(v, subset, dir);
			if (r != null)
				ret.add(r);
		}
		return new VertexResult(ret);
	}
}
