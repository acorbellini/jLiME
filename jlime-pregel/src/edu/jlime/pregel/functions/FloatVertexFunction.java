package edu.jlime.pregel.functions;

import edu.jlime.pregel.client.WorkerContext;
import edu.jlime.pregel.graph.VertexFunction;
import gnu.trove.map.hash.TLongFloatHashMap;

public interface FloatVertexFunction extends VertexFunction {
	void execute(long v, TLongFloatHashMap in, WorkerContext ctx)
			throws Exception;
}
