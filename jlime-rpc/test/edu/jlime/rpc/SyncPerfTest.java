package edu.jlime.rpc;

import java.util.HashSet;

import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.transport.Transport;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.metrics.metric.SensorMeasure;
import edu.jlime.rpc.fr.NACK;
import edu.jlime.rpc.message.StackElement;

public class SyncPerfTest {

	public static class SyncRemote {
		HashSet<Integer> vals = new HashSet<>();
		private Metrics rpc;

		public SyncRemote(Metrics rpc) {
			this.rpc = rpc;
		}

		public synchronized void addVal(int val) {
			if (val % 10000 == 0)
				System.out.println("Add " + val);
			if (vals.contains(val))
				System.out.println("Repeated " + val);
			vals.add(val);
		}

		public synchronized void finish() {
			System.out.println(rpc.toString());
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration config = new Configuration();

		config.port = 6070;
		config.mcastport = 5050;

		Metrics metrics = new Metrics("test");

		RPCDispatcher rpc = new JLiMEFactory(config).build();

		rpc.setMetrics(metrics);

		rpc.registerTarget("remote", new SyncRemote(metrics), true);

		rpc.start();
	}
}
