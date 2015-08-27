package edu.jlime.graphly.server;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.graphly.GraphlyConfiguration;
import edu.jlime.graphly.client.GraphlyClient;
import edu.jlime.graphly.http.WebServer;
import edu.jlime.graphly.storenode.GraphlyStoreNode;
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
import edu.jlime.rpc.NetworkConfiguration;

public class LocalGraphlyServer extends GraphlyServer {

	protected Logger log = Logger.getLogger(LocalGraphlyServer.class);

	NetworkConfiguration networkConfig;
	GraphlyConfiguration graphlyConfig;

	private JobDispatcher jobs;
	private RPCDispatcher rpc;
	private String storeLoc;
	private Boolean isCoord;
	private GraphlyCoordinatorImpl coord;
	private Integer rs;

	private CoordinatorServer pregel_coord;
	private WorkerServer pregel_worker;
	private GraphlyClient graphly;

	private WebServer ws;

	private GraphlyStoreNode storeNode;

	private Metrics mgr;

	public LocalGraphlyServer(NetworkConfiguration net,
			GraphlyConfiguration graphly, String storeLoc, Boolean isCoord,
			Integer rs) {
		this.networkConfig = net;
		this.graphlyConfig = graphly;
		this.storeLoc = storeLoc;
		this.isCoord = isCoord;
		this.rs = rs;
	}

	public void start() throws Exception {

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
		rpc = new JLiMEFactory(networkConfig, data, null).build();

		try {
			if (isCoord)
				new Thread("Coordinator Thread") {
					public void run() {
						try {
							// if (log.isDebugEnabled())
							log.info("Initializing Graphly Coordinator");
							LocalGraphlyServer.this.coord = new GraphlyCoordinatorImpl(
									rpc, rs);

							log.info("Initializing Pregel Coordinator");
							LocalGraphlyServer.this.pregel_coord = new CoordinatorServer(
									rpc);

							// if (log.isDebugEnabled())
							log.info("Finished starting coordinators");
						} catch (Exception e) {
							e.printStackTrace();
						}
					};
				}.start();

			log.info("Creating Graphly Store Node");
			this.storeNode = new GraphlyStoreNode(storeLoc, graphlyConfig, rpc);
			rpc.registerTarget("graphly", storeNode, false);

			log.info("Creating Pregel Worker Node");
			pregel_worker = new WorkerServer(rpc);

			log.info("Creating Job Node");
			jobs = JobDispatcher.build(0, rpc);

			jobs.start();

			rpc.start();

			this.mgr = new Metrics(rpc.getCluster().getLocalPeer().toString());
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
			this.graphly = GraphlyClient.build(rpc, pregelClient, jobs, rs);
			jobs.setGlobal("graphly", getGraphlyClient());

			if (log.isDebugEnabled())
				log.debug("Starting metrics");

			log.info("Graphly Server Fully Started on peer "
					+ rpc.getCluster().getLocalPeer());

			log.info("Starting Web Server");
			try {
				this.ws = new WebServer().port(8080);
				ws.start();
				log.info("Started Web Server on port " + ws.getPort());
			} catch (Exception e) {
				e.printStackTrace();
			}

		} catch (Exception e) {
			if (rpc != null)
				rpc.stop();
			throw e;
		}
	}

	public void stop() throws Exception {

		jobs.stop();

		rpc.stop();

		coord.stop();

		pregel_coord.stop();

		pregel_worker.stop();

		ws.stop();

		storeNode.stop();

		mgr.stop();

	}

	public GraphlyClient getGraphlyClient() {
		return graphly;
	}

}
