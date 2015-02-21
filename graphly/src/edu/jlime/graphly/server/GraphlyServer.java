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
import edu.jlime.jd.server.ClusterProvider;
import edu.jlime.jd.server.JobServer;
import edu.jlime.metrics.jmx.MetricsJMX;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.metrics.sysinfo.InfoProvider;
import edu.jlime.metrics.sysinfo.SysInfoProvider;
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

	public static void main(final String[] args) throws Exception {

		final String storeName = args[0];
		final String storeDir = args[1];
		final Integer localServers = Integer.valueOf(args[2]);
		Boolean coord = Boolean.valueOf(args[3]);
		final Integer remoteServers = Integer.valueOf(args[4]);

		if (coord)
			new Thread() {
				public void run() {
					try {
						new GraphlyCoordinatorImpl(remoteServers);
					} catch (Exception e) {
						e.printStackTrace();
					}
				};
			}.start();

		if (localServers == 1)
			createServer(storeName, storeDir, 0);
		else {
			ExecutorService svc = Executors.newCachedThreadPool();
			for (int i = 0; i < localServers; i++) {
				final int curr = i;
				svc.execute(new Runnable() {

					@Override
					public void run() {
						try {
							createServer(storeName, storeDir, curr);
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

	private static void createServer(String sname, String sdir, int i)
			throws Exception {
		new GraphlyServer(sname, sdir.replaceAll("\\$i", i + "")).start();
	}

	public void start() throws Exception {

		Map<String, String> data = new HashMap<>();
		data.put("app", "graphly");
		data.put("type", "server");

		rpc = new JLiMEFactory(data, new DataFilter("app", "graphly")).build();

		GraphlyStoreNode storeNode = new GraphlyStoreNode(storeName, storeLoc,
				rpc);

		rpc.registerTarget("graphly", storeNode, false);

		// rpc.setMetrics(mgr);

		rpc.start();

		jobs = JobServer.jLiME();
		jobs.getJd().setGlobal("graphly", Graphly.build(rpc, jobs.getJd(), 0));
		jobs.start();

		storeNode.setJobExecutorID(jobs.getJd().getLocalPeer());

		Metrics mgr = new Metrics(rpc.getCluster().getLocalPeer().getName());
		for (InfoProvider sysinfo : SysInfoProvider.get())
			sysinfo.load(mgr);
		new ClusterProvider(jobs.getJd()).load(mgr);
		MetricsJMX jmx = new MetricsJMX(mgr);
		jmx.start();
	}

	public void stop() throws Exception {
		rpc.stop();
		jobs.stop();
	}
}
