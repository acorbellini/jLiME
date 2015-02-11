package edu.jlime.graphly.server;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import edu.jlime.core.cluster.DataFilter;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.graphly.GraphlyStoreNode;
import edu.jlime.graphly.client.Graphly;
import edu.jlime.jd.server.JobServer;
import edu.jlime.rpc.JLiMEFactory;

public class GraphlyServer {
	private JobServer jobs;
	private RPCDispatcher rpc;
	private String storeName;
	private String storeLoc;

	public GraphlyServer(String storeName, String storeLoc) {
		this.storeName = storeName;
		this.storeLoc = storeLoc;
	}

	public static void main(String[] args) throws Exception {

		Integer servers = Integer.valueOf(args[1]);

		Boolean coord = Boolean.valueOf(args[3]);
		if (coord)
			new Thread() {
				public void run() {
					try {
						new GraphlyCoordinatorImpl(servers);
					} catch (Exception e) {
						e.printStackTrace();
					}
				};
			}.start();

		if (servers == 1)
			createServer(args, 0);
		else {
			ExecutorService svc = Executors.newCachedThreadPool();
			for (int i = 0; i < servers; i++) {
				final int curr = i;
				svc.execute(new Runnable() {

					@Override
					public void run() {
						try {
							createServer(args, curr);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				});
			}
			svc.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
			svc.shutdown();
		}

	}

	private static void createServer(String[] args, int i) throws Exception {
		new GraphlyServer(args[0], args[2].replaceAll("\\$i", i + "")).start();
	}

	public void start() throws Exception {
		Map<String, String> data = new HashMap<>();
		data.put("app", "graphly");
		data.put("type", "server");

		rpc = new JLiMEFactory(data, new DataFilter("app", "graphly")).build();

		GraphlyStoreNode storeNode = new GraphlyStoreNode(storeName, storeLoc,
				rpc);

		rpc.registerTarget("graphly", storeNode, false);

		rpc.start();

		jobs = JobServer.jLiME();
		jobs.getJd().setGlobal("graphly", Graphly.build(rpc, jobs.getJd(), 0));
		jobs.start();

		storeNode.setJobExecutorID(jobs.getJd().getLocalPeer());
	}

	public void stop() throws Exception {
		rpc.stop();
		jobs.stop();
	}
}
