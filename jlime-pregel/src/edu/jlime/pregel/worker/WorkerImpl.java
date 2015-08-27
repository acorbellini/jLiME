package edu.jlime.pregel.worker;

import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.pregel.client.SplitFunction;
import edu.jlime.pregel.coordinator.Aggregator;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.graph.rpc.Graph;
import edu.jlime.pregel.worker.rpc.Worker;

public class WorkerImpl implements Worker {

	ArrayList<WorkerTask> contexts = new ArrayList<>();

	private UUID id = UUID.randomUUID();

	private RPCDispatcher rpc;

	public WorkerImpl(RPCDispatcher rpc) {
		this.rpc = rpc;
	}

	@Override
	public UUID getID() throws Exception {
		return id;
	}

	@Override
	public void nextSuperstep(int superstep, int taskID, SplitFunction func,
			Map<String, Aggregator> aggregators) throws Exception {
		contexts.get(taskID).nextStep(superstep, func, aggregators);
	}

	@Override
	public void createTask(int taskID, Peer cli, VertexFunction<?> func,
			long[] vList, PregelConfig config) throws Exception {
		synchronized (contexts) {
			while (contexts.size() <= taskID)
				contexts.add(null);
			contexts.set(taskID, new WorkerTask(this, rpc, cli, func, vList,
					taskID, config));
		}
	}

	@Override
	public void execute(int taskID) throws Exception {
		contexts.get(taskID).execute();
	}

	public Graph getLocalGraph(String name) throws Exception {
		return (Graph) this.rpc.getTarget(name);
	}

	@Override
	public void cleanup(int taskID) throws Exception {
		WorkerTask task = contexts.get(taskID);
		if (task != null)
			task.cleanup();
		contexts.set(taskID, null);
	}

	@Override
	public void sendMessage(String msgType, long from, long to, Object val,
			int taskID) throws Exception {
		contexts.get(taskID).queueVertexData(msgType, from, to, val);
	}

	@Override
	public void sendFloatMessage(String msgType, long from, long to, float msg,
			int taskID) throws Exception {
		contexts.get(taskID).queueFloatVertexData(msgType, from, to, msg);
	}

	@Override
	public void sendBroadcastMessage(String msgType, long from, Object val,
			int taskID) throws Exception {
		contexts.get(taskID).queueBroadcastVertexData(msgType, from, val);
	}

	@Override
	public void sendFloatBroadcastMessage(String msgType, long from, float val,
			int taskID) throws Exception {
		contexts.get(taskID).queueBroadcastFloatVertexData(msgType, from, val);
	}

	@Override
	public void sendDoubleMessage(String msgType, long from, long to,
			double val, int taskid) throws Exception {
		contexts.get(taskid).queueDoubleVertexData(msgType, from, to, val);

	}

	@Override
	public void sendDoubleBroadcastMessage(String msgType, long from,
			double val, int taskid) throws Exception {
		contexts.get(taskid).queueBroadcastDoubleVertexData(msgType, from, val);
	}

	@Override
	public void sendFloatArrayMessage(String msgtype, long from, long to,
			float[] value, int taskid) throws Exception {
		contexts.get(taskid)
				.queueFloatArrayVertexData(msgtype, from, to, value);
	}

	@Override
	public void sendFloatArrayBroadcastMessage(String msgtype, long from,
			float[] value, int taskid) throws Exception {
		contexts.get(taskid).queueBroadcastFloatArrayVertexData(msgtype, from,
				value);
	}

	@Override
	public void sendObjectsMessage(String msgtype, long[] from, long[] to,
			Object[] objects, int taskid) throws Exception {
		WorkerTask workerTask = contexts.get(taskid);

		workerTask.queueVertexData(msgtype, from, to, objects);

	}

	@Override
	public void sendFloatArrayMessage(String msgType, long from, long[] to,
			float[][] vals, int taskid) throws Exception {
		WorkerTask workerTask = contexts.get(taskid);
		synchronized (workerTask) {
			for (int i = 0; i < to.length; i++) {
				workerTask.queueFloatArrayVertexData(msgType, from, to[i],
						vals[i]);
			}
		}
	}

	@Override
	public void sendFloatMessage(String msgType, long from, long[] to,
			float[] vals, int taskid) throws Exception {
		WorkerTask workerTask = contexts.get(taskid);
		workerTask.queueFloatVertexData(msgType, from, to, vals);

	}

	@Override
	public void sendDoubleMessage(String msgType, long from, long[] to,
			double[] vals, int taskid) throws Exception {
		WorkerTask workerTask = contexts.get(taskid);
		synchronized (workerTask) {
			for (int i = 0; i < to.length; i++) {
				workerTask.queueDoubleVertexData(msgType, from, to[i], vals[i]);
			}
		}
	}

	public void stop() {
	}

	@Override
	public void sendBroadcastMessageSubgraph(String msgType, String subGraph,
			long v, Object val, int taskid) {
		contexts.get(taskid).queueBroadcastSubgraphVertexData(msgType,
				subGraph, val);

	}

	@Override
	public void sendBroadcastMessageSubgraphFloat(String msgType,
			String subgraph, long v, float val, int taskid) throws Exception {
		contexts.get(taskid)
				.queueBroadcastSubgraphFloat(msgType, subgraph, val);
	}

	@Override
	public void finishedProcessing(int taskID) throws Exception {
		contexts.get(taskID).finishedProcessing();
	}
}
