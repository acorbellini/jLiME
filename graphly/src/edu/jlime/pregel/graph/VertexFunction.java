package edu.jlime.pregel.graph;

import java.io.Serializable;
import java.util.Iterator;

import edu.jlime.pregel.client.WorkerContext;
import edu.jlime.pregel.messages.PregelMessage;

public interface VertexFunction<T extends PregelMessage> extends Serializable {
	void execute(long v, Iterator<T> in, WorkerContext ctx) throws Exception;
}
