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
			Map<String, String> data) throws Exception {
		Peer p = cluster.getByAddress(from);
		if (p == null) {
			log.info("New member found : " + name + " id " + from);
			Peer peer = new Peer(from, name);
			peer.putData(data);
			cluster.addPeer(peer);
			failure.addPeerToMonitor(peer);
			onNewPeer(peer);
		}
	}

	@Override
	public void nodeFailed(Peer peer) {
		this.log.info("Node " + peer + " crashed. ");
		this.cluster.removePeer(peer);
		onFailedPeer(peer);
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

}
