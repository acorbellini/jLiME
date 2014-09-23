package edu.jlime.rpc;

import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.util.PerfMeasure;

public class SyncPerfTest {

	public static class SyncRemote {
		public String getString(Double d) {
			// PerfMeasure.takeTime("read", false);
			// new Exception().printStackTrace();
			try {
				// PerfMeasure.startTimer("write", 1000, false);
				return "Hello!";
			} finally {
				// PerfMeasure.takeTime("read", false);

			}

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration config = new Configuration();

		config.port = 6070;
		config.mcastport = 5050;

		RPCDispatcher rpc = new JlimeFactory(config).build();

		rpc.registerTarget("remote", new SyncRemote(), true);

		rpc.start();
	}
}
