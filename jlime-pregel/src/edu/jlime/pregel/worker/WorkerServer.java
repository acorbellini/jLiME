package edu.jlime.pregel.worker;

import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.rpc.Configuration;
import edu.jlime.rpc.JlimeFactory;

public class WorkerServer {
	public static void main(String[] args) throws Exception {
		Configuration config = new Configuration();
		config.port = 4040;
		config.mcastport = 5050;
		JlimeFactory fact = new JlimeFactory(config);		
		RPCDispatcher disp = fact.build();
		disp.registerTarget("Worker", new WorkerImpl());
	}
}
