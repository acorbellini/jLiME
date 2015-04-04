package edu.jlime.graphly.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.graphly.GraphlyStoreNode;
import edu.jlime.graphly.client.Graphly;
import edu.jlime.jd.JobDispatcher;
import edu.jlime.jd.server.ClusterProvider;
import edu.jlime.metrics.jmx.MetricsJMX;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.metrics.sysinfo.InfoProvider;
import edu.jlime.metrics.sysinfo.SysInfoProvider;
import edu.jlime.pregel.client.PregelClient;
import edu.jlime.pregel.coordinator.CoordinatorServer;
import edu.jlime.pregel.worker.WorkerServer;
import edu.jlime.rpc.JLiMEFactory;

public class GraphlyServer {
	private JobDispatcher jobs;
	private RPCDispatcher rpc;
	private String storeName;
	private String storeLoc;
	private Boolean isCoord;
	private GraphlyCoordinatorImpl coord;
	private Integer rs;
	protected Logger log = Logger.getLogger(GraphlyServer.class);

	private CoordinatorServer pregel_coord;
	private WorkerServer pregel_worker;

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

		Map<String, String> data = new HashMap<>();
		// data.put("app", "graphly");
		// data.put("type", "server");
		data.put("app", "graphly-server,"
				+ WorkerServer.WORKER_KEY
				+ ","
				+ JobDispatcher.SERVER
				+ (isCoord ? "," + GraphlyCoordinatorImpl.COORDINATOR + ","
						+ CoordinatorServer.COORDINATOR_KEY : ""));

		rpc = new JLiMEFactory(data, null).build();

		if (isCoord)
			new Thread() {
				public void run() {
					try {
						if (log.isDebugEnabled())
							log.debug("Initializing Coordinator");

						GraphlyServer.this.coord = new GraphlyCoordinatorImpl(
								rpc, rs);

						GraphlyServer.this.pregel_coord = new CoordinatorServer(
								rpc);

						if (log.isDebugEnabled())
							log.debug("Finished Initializing Coordinator");
					} catch (Exception e) {
						e.printStackTrace();
					}
				};
			}.start();

		GraphlyStoreNode storeNode = new GraphlyStoreNode(storeName, storeLoc,
				rpc);

		rpc.registerTarget("graphly", storeNode, false);

		pregel_worker = new WorkerServer(rpc);

		jobs = JobDispatcher.build(0, rpc);

		jobs.start();

		rpc.start();

		Metrics mgr = new Metrics(rpc.getCluster().getLocalPeer().toString());
		for (InfoProvider sysinfo : SysInfoProvider.get())
			sysinfo.load(mgr);

		new ClusterProvider(jobs).load(mgr);

		MetricsJMX jmx = new MetricsJMX(mgr);
		jmx.start();
		if (log.isDebugEnabled())
			log.debug("Finshed Starting metrics");

		jobs.setMetrics(mgr);

		PregelClient pregelClient = new PregelClient(rpc, rs);
		jobs.setGlobal("pregel", pregelClient);

		jobs.setGlobal("graphly", Graphly.build(rpc, pregelClient, jobs, rs));

		if (log.isDebugEnabled())
			log.debug("Starting metrics");

		log.info("Graphly Server Fully Started on peer "
				+ rpc.getCluster().getLocalPeer());
	}

	public void stop() throws Exception {

		jobs.stop();

		rpc.stop();

		pregel_worker.stop();

		if (coord != null)
			coord.stop();

		if (pregel_coord != null)
			pregel_coord.stop();
	}
}
