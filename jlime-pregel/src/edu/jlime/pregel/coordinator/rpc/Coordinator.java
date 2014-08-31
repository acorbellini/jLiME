package edu.jlime.pregel.coordinator.rpc;

import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import edu.jlime.core.rpc.Sync;
import edu.jlime.pregel.coordinator.Aggregator;
import edu.jlime.pregel.graph.PregelGraph;
import edu.jlime.pregel.graph.Vertex;
import edu.jlime.pregel.graph.VertexFunction;

public interface Coordinator {
	public void finished(UUID taskID, UUID workerID, Boolean processedWork)
			throws Exception;

	public PregelGraph execute(PregelGraph input, VertexFunction f,
			Integer supersteps, HashMap<String, Aggregator> aggregators,
			List<Vertex> vList) throws Exception;

	public Double getAggregatedValue(UUID taskID, Vertex v, String name)
			throws Exception;

	@Sync
	public void setAggregatedValue(UUID taskID, Vertex v, String name,
			Double val) throws Exception;
}
