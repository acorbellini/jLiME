package edu.jlime.rpc;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.cluster.PeerFilter;
import edu.jlime.core.transport.Address;
import edu.jlime.core.transport.Transport;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.rpc.data.DataListener;
import edu.jlime.rpc.data.DataMessage;
import edu.jlime.rpc.data.Response;

public class jLiMETransport extends Transport implements DataListener {

	private Logger log = Logger.getLogger(jLiMETransport.class);

	private Stack commStack;

	private ExecutorService handleExecutor = Executors
			.newCachedThreadPool(new ThreadFactory() {
				AtomicInteger cont = new AtomicInteger(0);

				@Override
				public Thread newThread(Runnable r) {
					Thread t = Executors.defaultThreadFactory().newThread(r);
					t.setName("jLiME Transport Thread "
							+ cont.getAndIncrement());
					t.setDaemon(true);
					return t;
				}
			});

	public jLiMETransport(Peer local, PeerFilter filter, Stack commStack) {
		super(local, filter, commStack.getDiscovery(), commStack
				.getFailureDetection(), commStack.getStreamer());
		this.commStack = commStack;
		this.commStack.getData().addDataListener(this);
	}

	@Override
	public void onStop() throws Exception {
		handleExecutor.shutdown();
		commStack.stop();
		// handleExecutor.shutdown();
	}

	@Override
	public void sendAsync(Peer peer, byte[] marshalled) throws Exception {
		commStack.getData().sendData(marshalled, (Address) peer.getAddress(),
				false);
	}

	@Override
	public byte[] sendSync(Peer peer, byte[] marshalled) throws Exception {
		if (log.isDebugEnabled())
			log.debug("Calling Synchronously " + peer + ", sending "
					+ marshalled.length + " b.");

		byte[] resp = commStack.getData().sendData(marshalled,
				peer.getAddress(), true);

		if (log.isDebugEnabled())
			log.debug("FINISHED synchronous call  to " + peer + ", response "
					+ (resp == null ? "NULL" : resp.length + " b."));

		return resp;
	}

	@Override
	public void messageReceived(final DataMessage msg, final Response handler) {
		// if (log.isDebugEnabled())
		// log.debug("Received data from processor");

		handleExecutor.execute(new Runnable() {
			public void run() {
				Address origin = msg.getFrom();
				byte[] buff = msg.getData();
				byte[] rsp = jLiMETransport.super.callTransportListener(origin,
						buff);

				// if (log.isDebugEnabled())
				// log.debug("Sending response using response handler: " +
				// handler);
				if (handler != null) {
					try {
						handler.sendResponse(rsp);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		});
	}

	@Override
	public void start() throws Exception {
		commStack.start();
	}

	@Override
	public void setMetrics(Metrics metrics) {
		super.setMetrics(metrics);
		this.commStack.setMetrics(metrics);
	}

	@Override
	protected void onFailedPeer(Peer peer) {
		commStack.cleanup((Address) peer.getAddress());
	}

	@Override
	public Object getRealAddress() {
		return commStack.getDiscovery().getAddresses();
	}

	public Stack getCommStack() {
		return commStack;
	}
}
