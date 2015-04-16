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
import edu.jlime.rpc.Configuration;
import edu.jlime.rpc.JLiMEFactory;

public class GraphlyServer {
	private JobDispatcher jobs;
	private RPCDispatcher rpc;
	private String storeLoc;
	private Boolean isCoord;
	private GraphlyCoordinatorImpl coord;
	private Integer rs;
	protected Logger log = Logger.getLogger(GraphlyServer.class);

	private CoordinatorServer pregel_coord;
	private WorkerServer pregel_worker;
	private Graphly graphly;

	public GraphlyServer(String storeLoc, Boolean isCoord, Integer rs) {
		this.storeLoc = storeLoc;
		this.isCoord = isCoord;
		this.rs = rs;
	}

	public static void main(final String[] args) throws Exception {
		final String storeDir = args[0];
		final Integer localServers = Integer.valueOf(args[1]);
		Boolean coord = Boolean.valueOf(args[2]);
		final Integer remoteServers = Integer.valueOf(args[3]);

		createServers(storeDir, localServers, coord, remoteServers);

	}

	public static ArrayList<GraphlyServer> createServers(final String storeDir,
			final Integer localServers, final Boolean coord,
			final Integer remoteServers) throws Exception, InterruptedException {

		final ArrayList<GraphlyServer> ret = new ArrayList<>();

		if (localServers == 1)
			ret.add(createServer(storeDir, 0, coord, remoteServers,
					new Configuration()));
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
							GraphlyServer srv = createServer(storeDir, curr,
									isCoord, remoteServers, new Configuration());
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

	public static GraphlyServer createServer(String sdir, int i,
			Boolean isCoord, Integer remoteServers, Configuration config)
			throws Exception {
		GraphlyServer g = new GraphlyServer(sdir.replaceAll("\\$i", i + ""),
				isCoord, remoteServers);
		g.start(config);
		return g;
	}

	public void start(Configuration config) throws Exception {

		log.info("Starting Graphly Server (remotes:" + rs + ", coord:"
				+ isCoord + ", store:" + storeLoc + ")");

		Map<String, String> data = new HashMap<>();
		// data.put("app", "graphly");
		// data.put("type", "server");
		data.put("app", "graphly-server,"
				+ WorkerServer.WORKER_KEY
				+ ","
				+ JobDispatcher.SERVER
				+ (isCoord ? "," + GraphlyCoordinatorImpl.COORDINATOR + ","
						+ CoordinatorServer.COORDINATOR_KEY : ""));
		log.info("Creating RPC");
		rpc = new JLiMEFactory(config, data, null).build();

		if (isCoord)
			new Thread() {
				public void run() {
					try {
						// if (log.isDebugEnabled())
						log.info("Initializing Graphly Coordinator");
						GraphlyServer.this.coord = new GraphlyCoordinatorImpl(
								rpc, rs);

						log.info("Initializing Pregel Coordinator");
						GraphlyServer.this.pregel_coord = new CoordinatorServer(
								rpc);

						// if (log.isDebugEnabled())
						log.info("Finished starting coordinators");
					} catch (Exception e) {
						e.printStackTrace();
					}
				};
			}.start();

		log.info("Creating Graphly Store Node");
		GraphlyStoreNode storeNode = new GraphlyStoreNode(storeLoc, rpc);
		rpc.registerTarget("graphly", storeNode, false);

		log.info("Creating Pregel Worker Node");
		pregel_worker = new WorkerServer(rpc);

		log.info("Creating Job Node");
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

		log.info("Creating Global Pregel Client");
		PregelClient pregelClient = new PregelClient(rpc, rs);
		jobs.setGlobal("pregel", pregelClient);

		log.info("Creating Global Graphly Client");
		this.setGraphly(Graphly.build(rpc, pregelClient, jobs, rs));
		jobs.setGlobal("graphly", getGraphly());

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

	public Graphly getGraphly() {
		return graphly;
	}

	public void setGraphly(Graphly graphly) {
		this.graphly = graphly;
	}
}
