package edu.jlime.graphly.traversal;

public class SizeStep implements Step {

	@Override
	public TraversalResult exec(TraversalResult before) throws Exception {
		return new ValueResult((float) before.vertices().size());
	}

}
