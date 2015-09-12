package edu.jlime.pregel.graph;

import java.io.Serializable;
import java.util.Iterator;

import edu.jlime.pregel.client.Context;
import edu.jlime.pregel.messages.PregelMessage;

public interface VertexFunction<T extends PregelMessage> extends Serializable {
	void execute(long v, Iterator<T> in, Context ctx) throws Exception;
}
