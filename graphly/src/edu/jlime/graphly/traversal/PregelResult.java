package edu.jlime.graphly.traversal;

import edu.jlime.pregel.PregelExecution;
import gnu.trove.set.hash.TLongHashSet;

public class PregelResult extends VertexResult {

	private PregelExecution res;

	public PregelResult(TLongHashSet vertices, PregelExecution res) {
		super(vertices);
		this.res = res;
	}

	public PregelExecution getRes() {
		return res;
	}

}
