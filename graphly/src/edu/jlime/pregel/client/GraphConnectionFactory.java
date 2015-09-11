package edu.jlime.pregel.client;

import java.io.Serializable;

import edu.jlime.core.rpc.RPC;
import edu.jlime.pregel.graph.rpc.PregelGraph;

public interface GraphConnectionFactory extends Serializable {
	PregelGraph getGraph(RPC rpc) throws Exception;
}
