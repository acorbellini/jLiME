package edu.jlime.pregel.worker;

import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.graph.rpc.Graph;
import edu.jlime.pregel.worker.rpc.Worker;

public class WorkerImpl implements Worker {

	HashMap<UUID, WorkerTask> contexts = new HashMap<>();

	private UUID id = UUID.randomUUID();

	// private ClientManager<Coordinator, CoordinatorBroadcast> coordCli;

	// private ClientManager<Worker, WorkerBroadcast> workerCli;

	private RPCDispatcher rpc;

	// private List<Worker> workers;

	// private WorkerBroadcast workerBroadcast;

	public WorkerImpl(RPCDispatcher rpc) {
		this.rpc = rpc;
		// this.workers = workerCli.getAll();
		// this.workerBroadcast = workerCli.broadcast();
	}

	@Override
	public void sendMessage(PregelMessage msg, UUID taskID) throws Exception {
		contexts.get(taskID).queueVertexData(msg);
	}

	@Override
	public UUID getID() throws Exception {
		return id;
	}

	@Override
	public void nextSuperstep(Integer superstep, UUID taskID) throws Exception {
		contexts.get(taskID).nextStep(superstep);
	}

	@Override
	public void createTask(UUID taskID, Peer cli, VertexFunction func,
			PregelConfig config, Set<Long> init) throws Exception {
		contexts.put(taskID, new WorkerTask(this, rpc, cli, func, taskID,
				config, init));
	}

	// @Override
	// public Graph getResult(UUID taskID) throws Exception {
	// return contexts.get(taskID).getResultGraph();
	// }

	// public Worker getWorker(Long v) {
	// return workers.get((int) (v % workers.size()));
	//
	// }

	@Override
	public void execute(UUID taskID) throws Exception {
		contexts.get(taskID).execute();
	}

	// public void sendAll(UUID taskid, PregelMessage pregelMessage)
	// throws Exception {
	// workerCli.broadcast().sendMessage(pregelMessage, taskid);
	//
	// }

	public Graph getLocalGraph(String name) {
		return (Graph) this.rpc.getTarget(name);
	}

	@Override
	public void sendMessages(List<PregelMessage> value, UUID taskid) {
		contexts.get(taskid).sendMessages(value);
	}

	// public Worker getWorker(long to, SplitFunction splitFunc) {
	// return workerCli.get(splitFunc.getPeer(to, workerCli.getPeers()));
	// }

}
