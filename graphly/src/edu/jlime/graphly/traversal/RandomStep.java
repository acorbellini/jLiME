package edu.jlime.graphly.traversal;

import edu.jlime.collections.adjacencygraph.get.Dir;

public class RandomStep implements Step<Long, Long> {

	private GraphlyTraversal tr;
	private Dir dir;

	public RandomStep(Dir dir, GraphlyTraversal tr) {
		this.dir = dir;
		this.tr = tr;
	}

	@Override
	public Long exec(Long before) throws Exception {
		return tr.getGraph().getRandomEdge(before, dir);
	}

}
