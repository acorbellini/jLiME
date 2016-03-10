package edu.jlime.pregel.worker;

import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPC;
import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.pregel.client.SplitFunction;
import edu.jlime.pregel.coordinator.Aggregator;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.graph.rpc.PregelGraph;
import edu.jlime.pregel.messages.PregelMessage;
import edu.jlime.pregel.worker.rpc.Worker;

public class WorkerImpl implements Worker {

	ArrayList<WorkerTask> contexts = new ArrayList<>();

	private UUID id = UUID.randomUUID();

	private RPC rpc;

	public WorkerImpl(RPC rpc) {
		this.rpc = rpc;
	}

	@Override
	public UUID getID() throws Exception {
		return id;
	}

	@Override
	public void nextSuperstep(int superstep, int taskID, SplitFunction func, Map<String, Aggregator> aggregators)
			throws Exception {
		contexts.get(taskID).nextStep(superstep, func, aggregators);
	}

	@Override
	public void createTask(int taskID, Peer cli, VertexFunction<PregelMessage> func, long[] vList, PregelConfig config)
			throws Exception {
		synchronized (contexts) {
			while (contexts.size() <= taskID)
				contexts.add(null);
			contexts.set(taskID, new WorkerTask(this, rpc, cli, func, vList, taskID, config));
		}
	}

	@Override
	public void execute(int taskID) throws Exception {
		contexts.get(taskID).execute();
	}

	public PregelGraph getLocalGraph(String name) throws Exception {
		return (PregelGraph) this.rpc.getTarget(name);
	}

	@Override
	public void cleanup(int taskID) throws Exception {
		WorkerTask task = contexts.get(taskID);
		if (task != null)
			task.cleanup();
		contexts.set(taskID, null);
	}

	@Override
	public void sendFloatMessage(String msgType, long from, long to, float msg, int taskID) throws Exception {
		contexts.get(taskID).queueFloatVertexData(msgType, from, to, msg);
	}

	@Override
	public void sendFloatBroadcastMessage(String msgType, long from, float val, int taskID) throws Exception {
		contexts.get(taskID).queueBroadcastFloatVertexData(msgType, from, val);
	}

	@Override
	public void sendFloatMessage(String msgType, long from, long[] to, float[] vals, int taskid) throws Exception {
		WorkerTask workerTask = contexts.get(taskid);
		workerTask.queueFloatVertexData(msgType, from, to, vals);

	}

	public void stop() {
	}

	@Override
	public void sendBroadcastMessageSubgraphFloat(String msgType, String subgraph, long v, float val, int taskid)
			throws Exception {
		contexts.get(taskid).queueBroadcastSubgraphFloat(msgType, subgraph, val);
	}
}
