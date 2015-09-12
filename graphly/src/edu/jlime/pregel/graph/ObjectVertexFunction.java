package edu.jlime.pregel.graph;

import java.io.Serializable;

import edu.jlime.pregel.client.Context;
import gnu.trove.map.hash.TLongObjectHashMap;

public interface ObjectVertexFunction extends Serializable {
	void execute(long v, TLongObjectHashMap<Object> in, Context ctx) throws Exception;
}
