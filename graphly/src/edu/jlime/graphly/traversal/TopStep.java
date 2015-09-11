package edu.jlime.graphly.traversal;

import org.apache.log4j.Logger;

public class TopStep implements Step {

	private int top;
	private Traversal tr;

	public TopStep(int top, Traversal tr) {
		this.top = top;
		this.tr = tr;
	}

	@Override
	public TraversalResult exec(TraversalResult before) throws Exception {
		Logger.getLogger(TopStep.class).info("Obtaining top " + top + " vertices.");
		return before.top(top);
	}

}
