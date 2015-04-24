package edu.jlime.pregel.worker;

import java.util.ArrayList;
import java.util.UUID;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.pregel.client.SplitFunction;
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
	public void nextSuperstep(int superstep, int taskID, SplitFunction func)
			throws Exception {
		contexts.get(taskID).nextStep(superstep, func);
	}

	@Override
	public void createTask(int taskID, Peer cli, VertexFunction func,
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

	public Graph getLocalGraph(String name) {
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
	public void sendMessage(long from, long to, Object val, int taskID)
			throws Exception {
		contexts.get(taskID).queueVertexData(from, to, val);
	}

	@Override
	public void sendFloatMessage(long from, long to, float msg, int taskID)
			throws Exception {
		contexts.get(taskID).queueFloatVertexData(from, to, msg);
	}

	@Override
	public void sendBroadcastMessage(long from, Object val, int taskID)
			throws Exception {
		contexts.get(taskID).queueBroadcastVertexData(from, val);
	}

	@Override
	public void sendFloatBroadcastMessage(long from, float val, int taskID)
			throws Exception {
		contexts.get(taskID).queueBroadcastFloatVertexData(from, val);
	}

	@Override
	public void sendFloatMessage(long from, long[] to, float[] vals, int taskid) {
		WorkerTask workerTask = contexts.get(taskid);
		for (int i = 0; i < to.length; i++) {
			workerTask.queueFloatVertexData(from, to[i], vals[i]);
		}

	}

	@Override
	public void sendDoubleMessage(long from, long[] to, double[] vals,
			int taskid) {
		WorkerTask workerTask = contexts.get(taskid);
		for (int i = 0; i < to.length; i++) {
			workerTask.queueDoubleVertexData(from, to[i], vals[i]);
		}
	}

	@Override
	public void sendDoubleMessage(long from, long to, double val, int taskid) {
		contexts.get(taskid).queueDoubleVertexData(from, to, val);

	}

	@Override
	public void sendDoubleBroadcastMessage(long from, double val, int taskid) {
		contexts.get(taskid).queueBroadcastDoubleVertexData(from, val);
	}
}
