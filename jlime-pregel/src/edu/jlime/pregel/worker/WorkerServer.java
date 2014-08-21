package edu.jlime.pregel.worker;

import java.util.HashMap;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.ClientManager;
import edu.jlime.core.rpc.PeerFilter;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.pregel.coordinator.rpc.Coordinator;
import edu.jlime.pregel.coordinator.rpc.CoordinatorBroadcast;
import edu.jlime.pregel.coordinator.rpc.CoordinatorFactory;
import edu.jlime.rpc.Configuration;
import edu.jlime.rpc.JlimeFactory;

public class WorkerServer {
	public static void main(String[] args) throws Exception {
		Configuration config = new Configuration();
		config.port = 4040;
		config.mcastport = 5050;
		
		HashMap<String, String> data = new HashMap<>();		
		data.put("type", "worker");
		
		JlimeFactory fact = new JlimeFactory(config);		
		RPCDispatcher disp = fact.build();
		
		ClientManager<Coordinator, CoordinatorBroadcast> coord = disp.manage(new CoordinatorFactory(disp, "coordinator"), new PeerFilter() {
			
			@Override
			public boolean verify(Peer p) {
				return p.getData("type").equals("coordinator");
			}
		});
		
		disp.registerTarget("worker", new WorkerImpl(coord.first()));
	}
}
