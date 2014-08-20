package edu.jlime.pregel.coordinator;

import java.util.List;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.ClientManager;
import edu.jlime.core.rpc.PeerFilter;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.pregel.worker.VertexFunction;
import edu.jlime.pregel.worker.Worker;
import edu.jlime.pregel.worker.WorkerFactory;

public class CoordinatorImpl implements Coordinator {

	private ClientManager<Worker> workers;
	
	private RPCDispatcher rpc;

	public CoordinatorImpl(RPCDispatcher rpc) {
		
		this.rpc = rpc;
		
		this.rpc.registerTarget("coordinator", this);

		this.workers = rpc.manage(new WorkerFactory(rpc, "worker"),
				new PeerFilter() {
					public boolean verify(Peer p) {
						if (p.getData("type").equals("worker"))
							return true;
						return false;
					}
				});
	}

	private Worker getWorker(int vertexID) {
		List<Worker> list = workers.getAll();
		return list.get(vertexID % list.size());

	}

	@Override
	public void finished(int worker) throws Exception {

	}

	@Override
	public void execute(List<Integer> vertex, VertexFunction func)
			throws Exception {
		for (Integer vertexID : vertex) {
			getWorker(vertexID).schedule(vertexID, func);
		}
	}

	public void start() throws Exception {
		rpc.start();
	}
}
