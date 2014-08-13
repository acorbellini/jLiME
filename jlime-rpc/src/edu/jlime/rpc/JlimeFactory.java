package edu.jlime.rpc;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.log4j.Logger;

import edu.jlime.core.cluster.Cluster;
import edu.jlime.core.cluster.Peer;
import edu.jlime.core.marshalling.TypeConverter;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.stream.RemoteInputStream;
import edu.jlime.core.stream.RemoteOutputStream;
import edu.jlime.jd.CloseListener;
import edu.jlime.jd.JobDispatcher;
import edu.jlime.jd.JobDispatcherFactory;
import edu.jlime.jd.StreamProvider;
import edu.jlime.rpc.discovery.DiscoveryListener;
import edu.jlime.rpc.discovery.DiscoveryProvider;
import edu.jlime.rpc.fd.FailureListener;
import edu.jlime.rpc.fd.FailureProvider;
import edu.jlime.rpc.message.Address;
import edu.jlime.rpc.np.Streamer;
import edu.jlime.util.ByteBuffer;

public class JlimeFactory extends JobDispatcherFactory {

	private int minPeers;

	private boolean isExec;

	private String[] tags;

	private Configuration config;

	Logger log = Logger.getLogger(JlimeFactory.class);

	public JlimeFactory(int minPeersBeforeReceiveData, String[] tags,
			boolean canReceiveJobs, Properties defProp) throws Exception {
		this.minPeers = minPeersBeforeReceiveData;
		this.isExec = canReceiveJobs;
		this.tags = tags;
		config = new Configuration(defProp);
	}

	@Override
	public JobDispatcher getJD() throws Exception {

		// First, we init the stack, the tie discovery with the cluster update.
		UUID id = UUID.randomUUID();
		// STACK
		final Stack commStack = config.getProtocol().equals("tcp") ? Stack
				.tcpStack(config, id) : Stack.udpStack(config, id);
		// Local peer
		PeerJlime localPeer = PeerJlime.newPeer(id, config.name);
		localPeer.putData(Peer.NAME, config.name);
		localPeer.putData(JobDispatcher.ISEXEC, Boolean.valueOf(isExec)
				.toString());
		localPeer.putData(JobDispatcher.TAGS, Arrays.toString(tags));

		// CLUSTER
		final Cluster cluster = new Cluster(localPeer);
		// RPC
		jLiMETransport tr = new jLiMETransport(commStack);
		RPCDispatcher rpc = new RPCDispatcher(cluster);
		rpc.setTransport(tr);
		rpc.getMarshaller().getTc()
				.registerTypeConverter(PeerJlime.class, new TypeConverter() {

					@Override
					public void toArray(Object o, ByteBuffer buff)
							throws Exception {
						PeerJlime pd = (PeerJlime) o;
						buff.putUUID(pd.getAddr().getId());
						buff.putString(pd.getName());
						HashMap<String, String> data = pd.getDataMap();
						buff.putMap(data);
					}

					@Override
					public Object fromArray(ByteBuffer buff, String originID,
							String clientID) throws Exception {
						UUID id = buff.getUUID();
						String name = buff.getString();
						Map<String, String> data = buff.getMap();
						PeerJlime pd = new PeerJlime(new Address(id), name);
						pd.putData(data);
						return pd;
					}
				});

		// JD
		final JobDispatcher disp = new JobDispatcher(minPeers, cluster, rpc);
		// Datos del JobDispatcher

		if (log.isDebugEnabled()) {
			log.debug("*********************************************");
			log.debug("* LOCAL ID : " + localPeer.getAddr() + "*");
			log.debug("*********************************************");
		}

		DiscoveryProvider disco = commStack.getDiscovery();
		disco.putData(localPeer.getDataMap());

		final FailureProvider fail = commStack.getFailureDetection();

		final Streamer streamer = commStack.getStreamer();

		disp.setStreamer(new StreamProvider() {

			@Override
			public RemoteOutputStream getOutputStream(UUID streamID,
					Peer streamSource) {
				return streamer.getOutputStream(streamID,
						((PeerJlime) streamSource).getAddr());
			}

			@Override
			public RemoteInputStream getInputStream(UUID streamID,
					Peer streamSource) {
				return streamer.getInputStream(streamID,
						((PeerJlime) streamSource).getAddr());
			}
		});
		// DISCOVERY
		disco.addListener(new DiscoveryListener() {

			@Override
			public synchronized void memberMessage(Address from,
					Map<String, String> data) throws Exception {
				PeerJlime p = (PeerJlime) cluster.getByID(from.getId()
						.toString());
				if (p != null)
					p.setAddr(from);
				else {
					log.info("New member found : " + data.get(Peer.NAME)
							+ " id " + from);
					PeerJlime peer = new PeerJlime(from, data.get(Peer.NAME));
					peer.putData(JobDispatcher.ISEXEC,
							data.get(JobDispatcher.ISEXEC));
					peer.putData(JobDispatcher.TAGS,
							data.get(JobDispatcher.TAGS));
					cluster.addPeer(peer);
					fail.addPeerToMonitor(peer);
				}

			}
		});

		fail.addFailureListener(new FailureListener() {
			@Override
			public void nodeFailed(PeerJlime peer) {
				log.info("Node " + peer + " crashed. ");
				cluster.removePeer(peer);
				commStack.cleanupOnFailedPeer(peer.getAddr());
			}
		});

		disp.addCloseListener(new CloseListener() {

			@Override
			public void onStop() throws Exception {
				commStack.stop();

			}
		});

		// commStack.start();

		return disp;
	}
}
