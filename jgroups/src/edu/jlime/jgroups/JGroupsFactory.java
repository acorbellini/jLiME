package edu.jlime.jgroups;

import java.io.InputStream;
import java.net.Inet4Address;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.blocks.MessageDispatcher;

import edu.jlime.core.cluster.IP;
import edu.jlime.core.cluster.Peer;
import edu.jlime.core.cluster.ServerAddressParser;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.RPCFactory;
import edu.jlime.core.transport.DiscoveryListener;
import edu.jlime.core.transport.DiscoveryProvider;
import edu.jlime.core.transport.FailureListener;
import edu.jlime.core.transport.FailureProvider;
import edu.jlime.util.NetworkUtils;
import edu.jlime.util.NetworkUtils.SelectedInterface;

public class JGroupsFactory implements RPCFactory {

	public static class JgroupsMembership implements DiscoveryProvider,
			FailureProvider {

		private FailureListener fail;
		private DiscoveryListener disco;

		@Override
		public void addListener(FailureListener l) {
			this.fail = l;
		}

		@Override
		public void addPeerToMonitor(Peer peer) throws Exception {
		}

		@Override
		public void addListener(DiscoveryListener l) {
			this.disco = l;

		}

		@Override
		public void putData(Map<String, String> dataMap) {
		}

		public void nodeDeleted(Peer p) {
			fail.nodeFailed(p);
		}

		public void nodeAdded(edu.jlime.core.transport.Address addr,
				String name, Map<String, String> data) throws Exception {
			disco.memberMessage(addr, name, data);
		}

	}

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

	private static List<Peer> convertToPeerList(List<Address> members) {
		List<Peer> ret = new ArrayList<>();
		for (Address m : members) {
			Peer srv;
			try {
				srv = PeerJgroups.createNew(m);
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
	public RPCDispatcher build() throws Exception {
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

		JChannel channel = new JChannel(jg);

		channel.setName(id);

		final JgroupsMembership member = new JgroupsMembership();

		ReceiverAdapter rcv = new ReceiverAdapter() {
			HashSet<Peer> current = new HashSet<Peer>();

			@Override
			public void viewAccepted(View view) {
				super.viewAccepted(view);

				List<Peer> peers = convertToPeerList(view.getMembers());

				HashSet<Peer> added = new HashSet<>(peers);
				HashSet<Peer> removed = new HashSet<>(current);
				added.removeAll(current);
				removed.retainAll(peers);
				for (Peer peer : added) {
					try {
						member.nodeAdded(peer.getAddress(), peer.getName(),
								peer.getDataMap());
					} catch (Exception e) {
						e.printStackTrace();
					}
				}

				for (Peer peer2 : removed) {
					member.nodeDeleted(peer2);
				}
			}
		};

		MessageDispatcher disp = new MessageDispatcher(channel, rcv, rcv);

		disp.asyncDispatching(true);

		channel.connect("DistributedExecutionService");

		Peer local = new Peer(
				new JGroupsAddress(disp.getChannel().getAddress()),
				ip.toString());

		JgroupsTransport tr = new JgroupsTransport(local, disp, member, null);

		RPCDispatcher rpc = new RPCDispatcher(tr);
		return rpc;
	}
}
