package edu.jlime.jd.server;

import java.util.HashMap;
import java.util.UUID;

import edu.jlime.core.cluster.DataFilter;
import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.stream.RemoteInputStream;
import edu.jlime.core.stream.RemoteOutputStream;
import edu.jlime.jd.JobDispatcher;
import edu.jlime.jd.StreamProvider;
import edu.jlime.metrics.jmx.MetricsJMX;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.metrics.sysinfo.InfoProvider;
import edu.jlime.metrics.sysinfo.SysInfoProvider;
import edu.jlime.rpc.Configuration;
import edu.jlime.rpc.JLiMEFactory;

public class JobServer {

	private JobDispatcher jd;
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

	private JobServer(JobDispatcher jd) throws Exception {
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
		jdData.put(JobDispatcher.ISEXEC, Boolean.valueOf(true).toString());
		jdData.put(JobDispatcher.TAGS, "Server");

		final RPCDispatcher rpc = new JLiMEFactory(new Configuration(), jdData,
				new DataFilter("app", JobDispatcher.SERVER, true)).build();

		// JD
		final JobDispatcher disp = JobDispatcher.build(0, rpc);

		return new JobServer(disp);
	}

	public void stop() throws Exception {
		jd.stop();
	}

	public JobDispatcher getJd() {
		return jd;
	}
}
