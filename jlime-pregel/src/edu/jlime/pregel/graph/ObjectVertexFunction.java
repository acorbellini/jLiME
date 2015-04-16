package edu.jlime.pregel.graph;

import edu.jlime.pregel.client.WorkerContext;
import gnu.trove.map.hash.TLongObjectHashMap;

import java.io.Serializable;

public interface ObjectVertexFunction extends Serializable {
	void execute(long v, TLongObjectHashMap<Object> in, WorkerContext ctx)
			throws Exception;
}
