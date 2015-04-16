package edu.jlime.rpc;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

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

		final RPCDispatcher rpc = new JLiMEFactory(config).build();

		rpc.start();

		final Cluster cl = rpc.getCluster();

		while (cl.size() < 2) {
			Thread.sleep(1000);
		}
		ArrayList<Peer> peers = cl.getPeers();
		Peer p = peers.get(0);
		if (p.equals(cl.getLocalPeer())) {
			p = peers.get(1);
		}
		final Peer server = p;
		int cont = 0;
		System.out.println("Starting Test");
		long init = System.currentTimeMillis();

		ExecutorService exec = Executors.newCachedThreadPool();
		final Semaphore sem = new Semaphore(200);

		for (int i = 0; i < ITERS; i++) {
			sem.acquire();
			// PerfMeasure.startTimer("write", 1000, false);
			// PerfMeasure.startTimer("data", 1000, false);

			if (cont % 10000 == 0)
				System.out.println(cont);

			cont++;
			final int curr = i;
			// System.out.println("Sending " + curr);
			exec.execute(new Runnable() {

				@Override
				public void run() {
					try {
						rpc.callSync(server, cl.getLocalPeer(), new MethodCall(
								"remote", "addVal", new Object[] { curr }));
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						sem.release();
					}

					// TODO Auto-generated method stub

				}
			});
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
		exec.shutdown();
		exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

		rpc.callSync(server, cl.getLocalPeer(), new MethodCall("remote",
				"finish", new Object[] {}));

		System.out.println("Time: "
				+ ((System.currentTimeMillis() - init) / (double) ITERS)
				+ " ms");

		rpc.stop();
	}

	private static void doSomething(int size) {
		size += 100;
	}
}
