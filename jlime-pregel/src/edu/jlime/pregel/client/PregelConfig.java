package edu.jlime.pregel.client;

import java.io.Serializable;
import java.util.HashMap;

import edu.jlime.pregel.coordinator.Aggregator;
import edu.jlime.pregel.graph.rpc.Graph;
import edu.jlime.pregel.worker.MessageMerger;

public class PregelConfig implements Serializable {
	private HashMap<String, Aggregator> aggregators = new HashMap<>();
	private MessageMerger merger;
	private SplitFunction split;
	private int threads = 8;
	private int maxSteps = 0;
	private boolean executeOnAll = false;
	private int queue_limit = 10000;
	private int segments = 32;
	private GraphConnectionFactory graph;

	public PregelConfig graph(GraphConnectionFactory graph) {
		this.graph = graph;
		return this;
	}

	public GraphConnectionFactory getGraph() {
		return graph;
	}

	public void setMaxSteps(int maxSteps) {
		this.maxSteps = maxSteps;
	}

	public void setAggregators(HashMap<String, Aggregator> aggregators) {
		this.aggregators = aggregators;
	}

	public PregelConfig executeOnAll(boolean executeOnAll) {
		this.executeOnAll = executeOnAll;
		return this;
	}

	public HashMap<String, Aggregator> getAggregators() {
		return aggregators;
	}

	public int getMaxSteps() {
		return maxSteps;
	}

	public boolean isExecuteOnAll() {
		return executeOnAll;
	}

	public PregelConfig steps(int i) {
		setMaxSteps(i);
		return this;
	}

	public PregelConfig aggregator(String k, Aggregator v) {
		aggregators.put(k, v);
		return this;
	}

	public PregelConfig split(SplitFunction s) {
		this.split = s;
		return this;
	}

	public PregelConfig merger(MessageMerger m) {
		this.merger = m;
		return this;
	}

	public SplitFunction getSplit() {
		return split;
	}

	public MessageMerger getMerger() {
		return merger;
	}

	public PregelConfig threads(int i) {
		this.threads = i;
		return this;
	}

	public int getThreads() {
		return threads;
	}

	public int getQueueLimit() {
		return queue_limit;
	}

	public PregelConfig queue(int queue_limit) {
		this.queue_limit = queue_limit;
		return this;
	}

	public int getSegments() {
		return segments;
	}

	public PregelConfig segments(int segments) {
		this.segments = segments;
		return this;
	}

	public static PregelConfig create() {
		return new PregelConfig();
	}

}
