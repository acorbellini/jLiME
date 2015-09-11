package edu.jlime.graphly.rec;

import java.io.Serializable;

import edu.jlime.graphly.traversal.Traversal;
import edu.jlime.graphly.traversal.Step;
import edu.jlime.graphly.traversal.TraversalResult;

public class CustomStep implements Step {

	public static interface CustomFunction extends Serializable {

		TraversalResult execute(TraversalResult before, Traversal tr) throws Exception;

	}

	private Traversal tr;
	private CustomFunction func;

	public CustomStep(Traversal tr, CustomFunction func) {
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

	public CustomFunction getFunction() {
		return func;
	}

}
