package edu.jlime.pregel.client;

import java.util.HashMap;
import java.util.Map.Entry;

import edu.jlime.pregel.PregelSubgraph;
import edu.jlime.pregel.coordinator.Aggregator;
import edu.jlime.pregel.graph.rpc.PregelGraph;
import edu.jlime.pregel.worker.WorkerTask;
import edu.jlime.util.Pair;
import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.map.hash.TObjectFloatHashMap;

public class Context {
	private WorkerTask task;

	private HashMap<String, TLongFloatHashMap> result = new HashMap();

	private TObjectFloatHashMap<String> broadcast = new TObjectFloatHashMap<>();

	private TObjectFloatHashMap<Pair<String, String>> sg_broadcast = new TObjectFloatHashMap<>();

	private HashMap<String, Aggregator> aggregators = new HashMap<>();

	private PregelConfig config;

	public Context(WorkerTask task) {
		this.task = task;
		this.config = task.getConfig();
		for (Entry<String, Aggregator> e : config.getAggregators().entrySet()) {
			aggregators.put(e.getKey(), e.getValue().copy());
		}
	}

	public PregelGraph getGraph() {
		return task.getGraph();
	};

	public Integer getSuperStep() {
		return task.getSuperStep();
	};

	public void sendFloat(String type, long to, float curr) throws Exception {
		TLongFloatHashMap map = result.get(type);
		if (map == null) {
			map = new TLongFloatHashMap();
			result.put(type, map);
		}
		config.getMerger(type).merge(to, curr, map);
	}

	public void sendAllFloat(String type, float val) throws Exception {
		config.getMerger(type).merge(type, val, broadcast);
	}

	public Aggregator getAggregator(String string) {
		return aggregators.get(string);
	}

	public PregelSubgraph getSubGraph(String string) {
		return task.getSubgraph(string);
	}

	public void sendAllFloatSubGraph(String msgType, String subgraph, float val)
			throws Exception {
		Pair<String, String> key = new Pair<String, String>(msgType, subgraph);
		config.getMerger(msgType).merge(key, val, sg_broadcast);

	}

	public ContextResult getResult() {
		return new ContextResult(result, broadcast, sg_broadcast, aggregators);
	}

	public HashMap<String, Aggregator> getAggregators() {
		return aggregators;
	}
}
