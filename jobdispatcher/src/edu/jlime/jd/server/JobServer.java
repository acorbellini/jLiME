package edu.jlime.jd.server;

import java.util.HashMap;
import java.util.UUID;

import org.apache.log4j.Logger;

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
import edu.jlime.rpc.JlimeFactory;
import edu.jlime.util.StringUtils;

public class JobServer {

	private JobDispatcher jd;
	private Metrics mgr;

	public static void main(String[] args) throws Exception {
		JobServer.jLiME().start();
	}

	public void start() throws Exception {
		
		jd.start();
		
		try {
			String[] info = new String[3];
			info[0] = mgr.get("sysinfo.os").toString();
			info[1] = mgr.get("jlime.interface").toString();
			info[2] = "Local Node : " +jd.getCluster().getLocalNode();
			Logger.getLogger(JobServer.class).info(
					StringUtils.printDEFTitle(info));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private JobServer(JobDispatcher jd) throws Exception {
		this.jd = jd;
		this.mgr = new Metrics();
		for (InfoProvider sysinfo : SysInfoProvider.get())
			sysinfo.load(mgr);
		new ClusterProvider(jd).load(mgr);
		jd.setMetrics(mgr);
		MetricsJMX jmx = new MetricsJMX(mgr);
		jmx.start();
	}

	public static JobServer jLiME() throws Exception {

		HashMap<String, String> jdData = new HashMap<>();
		jdData.put(JobDispatcher.ISEXEC, Boolean.valueOf(true).toString());
		jdData.put(JobDispatcher.TAGS, "Server");

		final RPCDispatcher rpc = new JlimeFactory(new Configuration(), jdData)
				.build();

		// JD
		final JobDispatcher disp = new JobDispatcher(0, rpc);
		disp.setStreamer(new StreamProvider() {

			@Override
			public RemoteOutputStream getOutputStream(UUID streamID,
					Peer streamSource) {
				return rpc.getStreamer().getOutputStream(streamID,
						streamSource.getAddress());
			}

			@Override
			public RemoteInputStream getInputStream(UUID streamID,
					Peer streamSource) {
				return rpc.getStreamer().getInputStream(streamID,
						streamSource.getAddress());
			}
		});
		return new JobServer(disp);
	}

	public void stop() throws Exception {
		jd.stop();
	}
}
