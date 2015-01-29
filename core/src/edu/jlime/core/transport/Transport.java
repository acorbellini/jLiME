package edu.jlime.core.transport;

import java.util.Map;

import org.apache.log4j.Logger;

import edu.jlime.core.cluster.Cluster;
import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.TransportListener;
import edu.jlime.metrics.metric.Metrics;

public abstract class Transport implements DiscoveryListener, FailureListener {
	private Logger log = Logger.getLogger(Transport.class);
	private Cluster cluster;
	private DiscoveryProvider disco;
	private FailureProvider failure;
	private TransportListener rcvr;
	private Metrics metrics;
	private Streamer streamer;

	public Transport(Peer local, DiscoveryProvider disco,
			FailureProvider failure, Streamer streamer) {
		// CLUSTER
		this.cluster = new Cluster(local);
		this.disco = disco;
		this.failure = failure;
		this.streamer = streamer;

		if (this.disco != null) {
			this.disco.putData(local.getDataMap());
			this.disco.addListener(this);
		}
		if (this.failure != null)
			this.failure.addListener(this);
	}

	@Override
	public void memberMessage(Address from, String name,
			Map<String, String> data, Object realAddress) throws Exception {
		Peer p = cluster.getByAddress(from);
		if (p == null) {
			// if (log.isDebugEnabled())
			log.info("New member found : " + name + " id " + from
					+ " with address " + realAddress);
			Peer peer = new Peer(from, name);
			peer.putData(data);
			cluster.addPeer(peer);
			failure.addPeerToMonitor(peer);
			onNewPeer(peer);
		}
	}

	@Override
	public void nodeFailed(Address addr) {
		this.log.info("Node " + addr + " crashed. ");
		Peer p = cluster.getByAddress(addr);
		if (p == null) {
			this.log.error("Node " + addr + " was not in cluster. ");
			return;
		}

		this.cluster.removePeer(p);
		onFailedPeer(p);
	}

	protected void onNewPeer(Peer peer) {

	}

	protected void onFailedPeer(Peer peer) {

	};

	public void setMetrics(Metrics metrics) {
		this.metrics = metrics;
	}

	public void listen(TransportListener rcvr) {
		this.rcvr = rcvr;
	}

	protected byte[] callTransportListener(Address addr, byte[] data) {
		return rcvr.process(addr, data);
	}

	public final Streamer getStreamer() {
		return streamer;
	};

	public Cluster getCluster() {
		return cluster;
	}

	public abstract void sendAsync(Peer p, byte[] marshalled) throws Exception;

	public abstract byte[] sendSync(Peer p, byte[] marshalled) throws Exception;

	public abstract void start() throws Exception;

	public abstract void stop() throws Exception;

	public abstract Object getRealAddress();

}
