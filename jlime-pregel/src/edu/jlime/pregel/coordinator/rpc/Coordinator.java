package edu.jlime.pregel.coordinator.rpc;

import java.util.UUID;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.Sync;
import edu.jlime.pregel.PregelExecution;
import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.pregel.graph.VertexFunction;

public interface Coordinator {
	public void finished(UUID taskID, UUID workerID, Boolean processedWork)
			throws Exception;

	public Double getAggregatedValue(UUID taskID, Long v, String name)
			throws Exception;

	@Sync
	public void setAggregatedValue(UUID taskID, Long v, String name, Double val)
			throws Exception;

	@Sync
	public PregelExecution execute(VertexFunction f, PregelConfig conf,
			Peer client) throws Exception;

}
