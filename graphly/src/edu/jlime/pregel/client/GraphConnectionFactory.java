package edu.jlime.pregel.client;

import java.io.Serializable;

import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.pregel.graph.rpc.Graph;

public interface GraphConnectionFactory extends Serializable {
	Graph getGraph(RPCDispatcher rpc) throws Exception;
}
