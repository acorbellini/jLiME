package edu.jlime.rpc;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.apache.log4j.Logger;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.DataReceiver;
import edu.jlime.core.rpc.Transport;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.rpc.data.DataListener;
import edu.jlime.rpc.data.DataProcessor.DataMessage;
import edu.jlime.rpc.data.Response;

public class jLiMETransport implements DataListener, Transport {

	private Logger log = Logger.getLogger(jLiMETransport.class);

	private DataReceiver rcvr;

	private ExecutorService handleExecutor = Executors.newFixedThreadPool(4,
			new ThreadFactory() {
				@Override
				public Thread newThread(Runnable r) {
					Thread t = Executors.defaultThreadFactory().newThread(r);
					t.setName("DEFRpc");
					return t;
				}
			});

	private Stack commStack;

	private Metrics metrics;

	public jLiMETransport(Stack commStack) {
		this.commStack = commStack;
		commStack.getData().addDataListener(this);
	}

	@Override
	public void stop() {
		handleExecutor.shutdown();
	}

	@Override
	public void sendAsync(Peer p, byte[] marshalled) throws Exception {
		PeerJlime pdef = (PeerJlime) p;
		commStack.getData().sendData(marshalled, pdef.getAddr(), false);
	}

	@Override
	public byte[] sendSync(Peer p, byte[] marshalled) throws Exception {
		if (log.isDebugEnabled())
			log.debug("Calling Synchronously " + p + ", sending "
					+ marshalled.length + " b.");

		PeerJlime pdef = (PeerJlime) p;
		byte[] resp = commStack.getData().sendData(marshalled, pdef.getAddr(),
				true);

		if (log.isDebugEnabled())
			log.debug("FINISHED synchronous call  to " + p + ", response "
					+ (resp == null ? "NULL" : resp.length + " b."));

		return resp;
	}

	@Override
	public void dataRcvd(final DataMessage msg, final Response handler) {
		if (log.isDebugEnabled())
			log.debug("Received data from processor");
		handleExecutor.execute(new Runnable() {
			public void run() {
				UUID origin = msg.getFrom().getId();
				byte[] buff = msg.getData();
				if (log.isDebugEnabled())
					log.debug("Unmarshalling data received");
				byte[] rsp = rcvr.process(origin.toString(), buff);

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
	public void registerReceiver(DataReceiver rcvr) {
		this.rcvr = rcvr;
	}

	@Override
	public void start() throws Exception {
		commStack.start();
	}

	@Override
	public void setMetrics(Metrics metrics) {
		this.metrics = metrics;
		this.commStack.setMetrics(metrics);
	}
}
