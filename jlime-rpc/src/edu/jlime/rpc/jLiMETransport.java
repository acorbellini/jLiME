package edu.jlime.rpc;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.apache.log4j.Logger;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.TransportListener;
import edu.jlime.core.transport.Address;
import edu.jlime.core.transport.Streamer;
import edu.jlime.core.transport.Transport;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.rpc.data.DataListener;
import edu.jlime.rpc.data.DataProcessor.DataMessage;
import edu.jlime.rpc.data.Response;
import edu.jlime.rpc.message.JLiMEAddress;

public class jLiMETransport extends Transport implements DataListener {

	private Logger log = Logger.getLogger(jLiMETransport.class);

	private Stack commStack;

	private ExecutorService handleExecutor = Executors.newFixedThreadPool(4,
			new ThreadFactory() {
				@Override
				public Thread newThread(Runnable r) {
					Thread t = Executors.defaultThreadFactory().newThread(r);
					t.setName("DEFRpc");
					return t;
				}
			});

	public jLiMETransport(Peer local, Stack commStack) {
		super(local, commStack.getDiscovery(), commStack.getFailureDetection(),
				commStack.getStreamer());
		this.commStack = commStack;
		this.commStack.getData().addDataListener(this);
	}

	@Override
	public void stop() throws Exception {
		commStack.stop();
		handleExecutor.shutdown();
	}

	@Override
	public void sendAsync(Peer pdef, byte[] marshalled) throws Exception {
		commStack.getData().sendData(marshalled,
				(JLiMEAddress) pdef.getAddress(), false);
	}

	@Override
	public byte[] sendSync(Peer pdef, byte[] marshalled) throws Exception {
		if (log.isDebugEnabled())
			log.debug("Calling Synchronously " + pdef + ", sending "
					+ marshalled.length + " b.");

		byte[] resp = commStack.getData().sendData(marshalled,
				(JLiMEAddress) pdef.getAddress(), true);

		if (log.isDebugEnabled())
			log.debug("FINISHED synchronous call  to " + pdef + ", response "
					+ (resp == null ? "NULL" : resp.length + " b."));

		return resp;
	}

	@Override
	public void messageReceived(final DataMessage msg, final Response handler) {
		if (log.isDebugEnabled())
			log.debug("Received data from processor");
		handleExecutor.execute(new Runnable() {
			public void run() {
				Address origin = msg.getFrom();
				byte[] buff = msg.getData();
				if (log.isDebugEnabled())
					log.debug("Unmarshalling data received");
				byte[] rsp = jLiMETransport.super.callTransportListener(origin,
						buff);
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
		commStack.cleanup((JLiMEAddress) peer.getAddress());
	}
}
