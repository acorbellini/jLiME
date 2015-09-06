package edu.jlime.pregel.graph;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.jlime.pregel.client.WorkerContext;
import edu.jlime.pregel.messages.PregelMessage;

public class MultiStepVertexFunction<T extends PregelMessage> implements
		VertexFunction<T> {

	List<VertexFunction> vFunc = new ArrayList<>();

	@Override
	public void execute(long v, Iterator<T> in, WorkerContext ctx)
			throws Exception {
		int curr = ctx.getSuperStep() % vFunc.size();
		vFunc.get(curr).execute(v, in, ctx);
	}

	public MultiStepVertexFunction step(VertexFunction v) {
		vFunc.add(v);
		return this;
	}

}
