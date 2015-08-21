package edu.jlime.graphly.traversal;

import gnu.trove.set.hash.TLongHashSet;

import java.util.Arrays;

import org.apache.log4j.Logger;

public class VarFilterStep implements Step {

	private String[] kList;
	private GraphlyTraversal g;
	private boolean neg;

	public VarFilterStep(String[] k, GraphlyTraversal g, boolean neg) {
		this.kList = k;
		this.g = g;
		this.neg = neg;
	}

	@Override
	public TraversalResult exec(TraversalResult before) throws Exception {
		Logger log = Logger.getLogger(VarFilterStep.class);
		TraversalResult res = before;
		int size = new TLongHashSet(before.vertices()).size();
		for (String k : kList) {
			TLongHashSet filter = ((TraversalResult) g.get(k)).vertices();

			if (neg)
				res = res.removeAll(filter);
			else
				res = res.retainAll(filter);
		}
		log.info("Filtered " + size + ", left with : "+ res.vertices().size());
		return res;
	}

	@Override
	public String toString() {
		return "VarFilterStep [kList=" + Arrays.toString(kList) + ", neg="
				+ neg + "]";
	}

}
