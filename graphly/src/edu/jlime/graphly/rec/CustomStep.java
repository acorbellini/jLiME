package edu.jlime.graphly.rec;

import java.io.Serializable;

import edu.jlime.graphly.traversal.GraphlyTraversal;
import edu.jlime.graphly.traversal.Step;
import edu.jlime.graphly.traversal.TraversalResult;

public class CustomStep implements Step {

	public static interface CustomFunction extends Serializable {

		TraversalResult execute(TraversalResult before, GraphlyTraversal tr)
				throws Exception;

	}

	private GraphlyTraversal tr;
	private CustomFunction func;

	public CustomStep(GraphlyTraversal tr, CustomFunction func) {
		this.tr = tr;
		this.func = func;
	}

	@Override
	public TraversalResult exec(TraversalResult before) throws Exception {
		return func.execute(before, tr);
	}

	@Override
	public String toString() {
		return "CustomStep [func=" + func + "]";
	}

}
