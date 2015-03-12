package edu.jlime.rpc;

import java.util.ArrayList;

import edu.jlime.core.cluster.Cluster;
import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.MethodCall;
import edu.jlime.core.rpc.RPCDispatcher;

public class SyncPerfClient {
	private static final int ITERS = 1000000;

	public static void main(String[] args) throws Exception {

		// System.in.read();

		Configuration config = new Configuration();
		config.port = 6070;
		config.mcastport = 5050;

		RPCDispatcher rpc = new JLiMEFactory(config).build();

		rpc.start();

		Cluster cl = rpc.getCluster();
		
		while (cl.size() < 2) {
			Thread.sleep(1000);
		}
		ArrayList<Peer> peers = cl.getPeers();
		Peer p = peers.get(0);
		if (p.equals(cl.getLocalPeer())) {
			p = peers.get(1);
		}
		long init = System.currentTimeMillis();
		for (int i = 0; i < ITERS; i++) {
			// PerfMeasure.startTimer("write", 1000, false);
			// PerfMeasure.startTimer("data", 1000, false);
			rpc.callSync(p, cl.getLocalPeer(), new MethodCall("remote",
					"getString", new Object[] { 15d }));

			// PerfMeasure.takeTime("read", false);

			// byte[] array = rpc.getMarshaller().toByteArray(new MethodCall(
			// "remote", "getString", new Object[] { 15d }));
			// int size = array.length;
			// doSomething(size);
			//
			// Object unmarshall =rpc.getMarshaller().getObject(array,
			// cl.getLocalPeer());
			// doSomething(unmarshall.hashCode());
		}
		System.out.println("Time: "
				+ ((System.currentTimeMillis() - init) / (double) ITERS)
				+ " ms");

		// rpc.stop();
	}

	private static void doSomething(int size) {
		size += 100;
	}
}
