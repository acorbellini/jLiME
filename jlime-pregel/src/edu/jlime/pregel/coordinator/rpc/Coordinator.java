package edu.jlime.pregel.coordinator.rpc;

import java.util.UUID;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.Sync;
import edu.jlime.pregel.PregelExecution;
import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.pregel.graph.VertexFunction;

public interface Coordinator {
	public void finished(int taskID, UUID workerID, Boolean processedWork)
			throws Exception;

	public Double getAggregatedValue(int taskID, Long v, String name)
			throws Exception;

	@Sync
	public void setAggregatedValue(int taskID, Long v, String name, Double val)
			throws Exception;

	@Sync
	public PregelExecution execute(VertexFunction f, long[] vList,
			PregelConfig conf, Peer client) throws Exception;

}
