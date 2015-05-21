package edu.jlime.pregel.client;

import java.io.Serializable;
import java.util.HashMap;

import edu.jlime.pregel.coordinator.Aggregator;
import edu.jlime.pregel.coordinator.HaltCondition;
import edu.jlime.pregel.mergers.MessageMerger;

public class PregelConfig implements Serializable {

	private HashMap<String, Aggregator> aggregators = new HashMap<>();

	private HashMap<String, MessageMerger> mergers = new HashMap<>();

	private SplitFunction split;
	private Integer threads = null;

	private int maxSteps = 0;
	private boolean executeOnAll = false;
	private String queue_limit = "auto";
	private GraphConnectionFactory graph;
	private int bQueue = 100;
	private Integer send_threads = null;

	private boolean persitentVertexList = true;
	private boolean persitentCurrentSplitList = true;

	private HaltCondition condition = null;

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

	public PregelConfig merger(String type, MessageMerger m) {
		this.mergers.put(type, m);
		return this;
	}

	public SplitFunction getSplit() {
		return split;
	}

	public MessageMerger getMerger(String type) {
		return this.mergers.get(type);
	}

	public PregelConfig threads(int i) {
		this.threads = i;
		return this;
	}

	public int getThreads() {
		if (threads == null)
			return Runtime.getRuntime().availableProcessors();
		return threads;
	}

	public String getQueueLimit() {
		return queue_limit;
	}

	public PregelConfig queue(String queue_limit) {
		this.queue_limit = queue_limit;
		return this;
	}

	public static PregelConfig create() {
		return new PregelConfig();
	}

	public PregelConfig broadcastQueue(int i) {
		this.bQueue = i;
		return this;
	}

	public int getBroadcastQueue() {
		return bQueue;
	}

	public int getSegments() {
		int t = 0;
		if (queue_limit.equals("auto")) {
			t = getThreads() * 64;
		} else
			t = Integer.valueOf(queue_limit.substring(0,
					queue_limit.indexOf("x")));
		// System.out.println("Segments " + t);

		return t;
	}

	public int getQueueSize() {
		int i = 0;
		if (queue_limit.equals("auto")) {
			float max = Runtime.getRuntime().maxMemory() * .5f;
			float entry_size = 8f + 8f + 64f;
			i = (int) ((max / (entry_size)) / getSegments());
		} else
			i = Integer.valueOf(queue_limit.substring(
					queue_limit.indexOf("x") + 1, queue_limit.length()));
		// System.out.println("Queue size" + i);
		return i;
	}

	public int getSendThreads() {
		if (send_threads == null)
			return Runtime.getRuntime().availableProcessors();
		return send_threads;
	}

	public PregelConfig sendthreads(int send_threads) {
		this.send_threads = send_threads;
		return this;
	}

	public PregelConfig persistVList(boolean persist) {
		this.persitentVertexList = persist;
		return this;
	}

	public PregelConfig persistCurrSplit(boolean persist) {
		this.persitentCurrentSplitList = persist;
		return this;
	}

	public boolean isPersitentCurrentSplitList() {
		return persitentCurrentSplitList;
	}

	public boolean isPersitentVertexList() {
		return persitentVertexList;
	}

	public PregelConfig haltCondition(HaltCondition cond) {
		this.condition = cond;
		return this;

	}

	public HaltCondition getHaltCondition() {
		return condition;
	}
}
