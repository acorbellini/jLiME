package edu.jlime.jgroups;

import java.io.InputStream;
import java.net.Inet4Address;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.jgroups.Address;
import org.jgroups.View;

import edu.jlime.core.cluster.Cluster;
import edu.jlime.core.cluster.IP;
import edu.jlime.core.cluster.Peer;
import edu.jlime.core.cluster.ServerAddressParser;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.jd.JobDispatcher;
import edu.jlime.jd.JobDispatcherFactory;
import edu.jlime.util.NetworkUtils;
import edu.jlime.util.NetworkUtils.SelectedInterface;

public class JGroupsFactory extends JobDispatcherFactory {

	public static enum JGroupsConfigType {

		TCPBAD("jgroups-tcp-bad-connection.xml"),

		UDPBAD("jgroups-udp-bad-connection.xml"),

		TCP("jgroups-tcp.xml"),

		UDP("jgroups-udp.xml");

		private String file;

		private JGroupsConfigType(String file) {
			this.file = file;
		}

		public String getFile() {
			return file;
		}
	}

	private InputStream jg;

	private int minPeers;

	private String[] tags;

	private boolean exec;

	public JGroupsFactory(InputStream jg, int minPeers, String[] tags,
			boolean isExec) {
		this.jg = jg;
		this.minPeers = minPeers;
		this.tags = tags;
		this.exec = isExec;
	}

	private static JGroupsConfigType jgroupsfile = JGroupsConfigType.TCP;

	public static void setJgroupsType(JGroupsConfigType jgroupsfile) {
		JGroupsFactory.jgroupsfile = jgroupsfile;
	}

	private static List<Peer> convertToPeerList(List<Address> members,
			JobDispatcher disp) {
		List<Peer> ret = new ArrayList<>();
		for (Address m : members) {
			PeerJgroups srv;
			try {
				srv = PeerJgroups.createNew(m, disp);
				ret.add(srv);
			} catch (Exception e) {
				Logger log = Logger.getLogger(JGroupsFactory.class);
				log.info("Address parse error" + e.getMessage());
			}
		}
		return ret;
	}

	static Logger log = Logger.getLogger(JGroupsFactory.class);

	public static InputStream getConfig() {
		String jgConfig = System.getProperty("def.tp");
		if (jgConfig != null) {
			setJgroupsType(JGroupsConfigType.valueOf(jgConfig));
		}
		return JGroupsFactory.class.getResourceAsStream(jgroupsfile.getFile());
	}

	@Override
	public JobDispatcher getJD() throws Exception {
		List<SelectedInterface> addrList = NetworkUtils.getLocalAddressIPv4();

		if (addrList.size() == 0)
			throw new Exception("No available interface");
		StringBuilder ifaces = new StringBuilder();
		for (SelectedInterface selectedInterface : addrList) {
			ifaces.append("," + selectedInterface.getNif().getName());
		}

		if (System.getProperty("def.if") == null)
			System.setProperty("def.if", addrList.get(0).getNif().getName());
		String iface = System.getProperty("def.if");

		StringBuilder ifacesLong = new StringBuilder();
		ifacesLong.append("\nAvailable interfaces (using interface " + iface
				+ "):\n");
		for (SelectedInterface selectedInterface : addrList)
			ifacesLong.append(selectedInterface.getNif().getName() + " ("
					+ selectedInterface.getNif().getDisplayName() + " - IP "
					+ selectedInterface.getInet().getHostAddress() + ")\n");
		System.setProperty("def.hostname", Inet4Address.getLocalHost()
				.getHostName());
		System.setProperty("def.iface_list", ifaces.substring(1));

		System.setProperty("java.net.preferIPv4Stack", "true");
		IP ip = IP.toIP(addrList.get(0).getInet().getHostAddress());
		String id = ServerAddressParser.generate(tags, ip, exec);

		PeerJgroups local = new PeerJgroups(id, ip, null);

		final Cluster cluster = new Cluster(local);

		JgroupsTransport tr = new JgroupsTransport(id, jg);

		RPCDispatcher rpc = new RPCDispatcher(cluster);

		rpc.setTransport(tr);

		final JobDispatcher disp = new JobDispatcher(minPeers, cluster, rpc);

		// ,tags, exec, null, disp);

		cluster.addPeer(local);

		tr.onViewChange(new OnViewChangeListener() {

			@Override
			public void viewChanged(View view) {
				cluster.update(convertToPeerList(view.getMembers(), disp));
			}
		});

		return disp;
	}
}
