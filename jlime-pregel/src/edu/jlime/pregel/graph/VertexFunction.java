package edu.jlime.pregel.graph;

import java.io.Serializable;
import java.util.Iterator;

import edu.jlime.pregel.client.WorkerContext;
import edu.jlime.pregel.messages.PregelMessage;

public interface VertexFunction extends Serializable {
	void execute(long v, Iterator<PregelMessage> in, WorkerContext ctx)
			throws Exception;
}
