package edu.jlime.graphly.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import edu.jlime.core.cluster.DataFilter;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.RPCDispatcher.RPCStatus;
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
	private Boolean isCoord;
	private GraphlyCoordinatorImpl coord;
	private Integer rs;
	protected Logger log = Logger.getLogger(GraphlyServer.class);

	public GraphlyServer(String storeName, String storeLoc, Boolean isCoord,
			Integer rs) {
		this.storeName = storeName;
		this.storeLoc = storeLoc;
		this.isCoord = isCoord;
		this.rs = rs;
	}

	public static void main(final String[] args) throws Exception {

		final String storeName = args[0];
		final String storeDir = args[1];
		final Integer localServers = Integer.valueOf(args[2]);
		Boolean coord = Boolean.valueOf(args[3]);
		final Integer remoteServers = Integer.valueOf(args[4]);

		createServers(storeName, storeDir, localServers, coord, remoteServers);

	}

	public static ArrayList<GraphlyServer> createServers(
			final String storeName, final String storeDir,
			final Integer localServers, final Boolean coord,
			final Integer remoteServers) throws Exception, InterruptedException {

		final ArrayList<GraphlyServer> ret = new ArrayList<>();

		if (localServers == 1)
			ret.add(createServer(storeName, storeDir, 0, coord, remoteServers));
		else {
			ExecutorService svc = Executors.newCachedThreadPool();
			for (int i = 0; i < localServers; i++) {
				final int curr = i;
				svc.execute(new Runnable() {

					@Override
					public void run() {
						try {
							boolean isCoord = false;
							if (curr == 0 && coord)
								isCoord = true;
							GraphlyServer srv = createServer(storeName,
									storeDir, curr, isCoord, remoteServers);
							synchronized (ret) {
								ret.add(srv);
							}

						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				});
			}
			svc.shutdown();
			svc.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
		}
		return ret;
	}

	public static GraphlyServer createServer(String sname, String sdir, int i,
			Boolean isCoord, Integer remoteServers) throws Exception {
		GraphlyServer g = new GraphlyServer(sname, sdir.replaceAll("\\$i", i
				+ ""), isCoord, remoteServers);
		g.start();
		return g;
	}

	public void start() throws Exception {

		if (isCoord)
			new Thread() {
				public void run() {
					try {
						log.info("Initializing Coordinator");
						GraphlyServer.this.coord = new GraphlyCoordinatorImpl(
								rs);
						log.info("Finished Initializing Coordinator");
					} catch (Exception e) {
						e.printStackTrace();
					}
				};
			}.start();

		Map<String, String> data = new HashMap<>();
		data.put("app", "graphly");
		data.put("type", "server");

		rpc = new JLiMEFactory(data, new DataFilter("app", "graphly")).build();

		GraphlyStoreNode storeNode = new GraphlyStoreNode(storeName, storeLoc,
				rpc);

		rpc.registerTarget("graphly", storeNode, false);

		// rpc.setMetrics(mgr);

		rpc.start();

		log.info("Initializing Job Dispatcher");
		jobs = JobServer.jLiME();
		log.info("Finished Initializing Job Dispatcher");

		log.info("Adding Graphly as global");
		jobs.getJd().setGlobal("graphly", Graphly.build(rpc, jobs.getJd(), rs));
		log.info("Finished adding Graphly as global");

		log.info("Starting Job Dispatcher");
		jobs.start();
		log.info("Finished starting Job Dispatcher");

		storeNode.setJobExecutorID(jobs.getJd().getLocalPeer());
		log.info("Starting metrics");
		Metrics mgr = new Metrics(rpc.getCluster().getLocalPeer().getName());
		for (InfoProvider sysinfo : SysInfoProvider.get())
			sysinfo.load(mgr);
		new ClusterProvider(jobs.getJd()).load(mgr);
		MetricsJMX jmx = new MetricsJMX(mgr);
		jmx.start();
		log.info("Finshed Starting metrics");
	}

	public void stop() throws Exception {
		rpc.stop();
		jobs.stop();
		if (coord != null)
			coord.stop();
	}
}
