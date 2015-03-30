package edu.jlime.pregel.client;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Set;

import edu.jlime.pregel.coordinator.Aggregator;
import edu.jlime.pregel.graph.rpc.Graph;
import edu.jlime.pregel.worker.MessageMerger;

public class PregelConfig implements Serializable {
	SplitFunction split;
	int maxSteps = 0;

	HashMap<String, Aggregator> aggregators = new HashMap<>();
	boolean executeOnAll = false;

	Set<Long> vList = null;
	Graph graph;

	private MessageMerger merger;
	private Integer threads;

	public PregelConfig graph(Graph graph) {
		this.graph = graph;
		return this;
	}

	public Graph getGraph() {
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

	public PregelConfig setvList(Set<Long> vList) {
		this.vList = vList;
		return this;
	}

	public HashMap<String, Aggregator> getAggregators() {
		return aggregators;
	}

	public int getMaxSteps() {
		return maxSteps;
	}

	public Set<Long> getvList() {
		return vList;
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

	public Integer getThreads() {
		return threads;
	}
}
