package edu.jlime.jd.server;

import java.util.HashMap;

import edu.jlime.core.cluster.DataFilter;
import edu.jlime.core.rpc.RPC;
import edu.jlime.jd.Dispatcher;
import edu.jlime.metrics.jmx.MetricsJMX;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.metrics.sysinfo.InfoProvider;
import edu.jlime.metrics.sysinfo.SysInfoProvider;
import edu.jlime.rpc.JLiMEFactory;
import edu.jlime.rpc.NetworkConfiguration;

public class JobServer {

	private Dispatcher jd;
	private Metrics mgr;

	public static void main(String[] args) throws Exception {
		JobServer.jLiME().start();
	}

	public void start() throws Exception {

		jd.start();

		try {
			// String[] info = new String[3];
			// info[0] = mgr.get("sysinfo.os").toString();
			// info[1] = mgr.get("jlime.interface").toString();
			// info[2] = "Local Node : " + jd.getCluster().getLocalNode();
			// Logger.getLogger(JobServer.class)
			// .info(StringUtils.printTitle(info));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private JobServer(Dispatcher jd) throws Exception {
		this.jd = jd;
		this.mgr = new Metrics(jd.getLocalPeer().toString());
		for (InfoProvider sysinfo : SysInfoProvider.get())
			sysinfo.load(mgr);
		new ClusterProvider(jd).load(mgr);
		jd.setMetrics(mgr);
		MetricsJMX jmx = new MetricsJMX(mgr);
		jmx.start();
	}

	public static JobServer jLiME() throws Exception {

		HashMap<String, String> jdData = new HashMap<>();
		jdData.put("app", "jobdispatcher");
		jdData.put(Dispatcher.ISEXEC, Boolean.valueOf(true).toString());
		jdData.put(Dispatcher.TAGS, "Server");

		final RPC rpc = new JLiMEFactory(new NetworkConfiguration(), jdData,
				new DataFilter("app", Dispatcher.SERVER, true)).build();

		// JD
		final Dispatcher disp = Dispatcher.build(0, rpc);

		return new JobServer(disp);
	}

	public void stop() throws Exception {
		jd.stop();
	}

	public Dispatcher getJd() {
		return jd;
	}
}
