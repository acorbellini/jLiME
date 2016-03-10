package edu.jlime.pregel.coordinator.rpc;

import java.util.Map;
import java.util.UUID;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.Sync;
import edu.jlime.pregel.PregelExecution;
import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.pregel.coordinator.Aggregator;
import edu.jlime.pregel.graph.VertexFunction;

public interface Coordinator {

	public void finished(int taskID, UUID workerID, Boolean processedWork, Map<String, Aggregator> aggregators)
			throws Exception;

	@Sync
	public PregelExecution execute(VertexFunction<?> f, long[] vList, PregelConfig conf, Peer client) throws Exception;

	public void error(Exception e, int taskid, UUID id) throws Exception;;

}
