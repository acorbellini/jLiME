package edu.jlime.graphly.traversal;

import edu.jlime.graphly.rec.VertexFilter;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.set.hash.TLongHashSet;

public class CustomFilterStep implements Step {

	private VertexFilter f;
	private GraphlyTraversal tr;

	public CustomFilterStep(VertexFilter f, GraphlyTraversal tr) {
		this.f = f;
		this.tr = tr;
	}

	@Override
	public TraversalResult exec(TraversalResult before) throws Exception {
		TLongHashSet ret = new TLongHashSet();
		TLongIterator it = before.vertices().iterator();
		while (it.hasNext()) {
			long next = it.next();
			if (f.filter(next, tr.getGraph())) {
				ret.add(next);
			}

		}
		return new VertexResult(ret);
	}

}