package edu.jlime.jgroups;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.Message.Flag;
import org.jgroups.ReceiverAdapter;
import org.jgroups.SuspectedException;
import org.jgroups.TimeoutException;
import org.jgroups.View;
import org.jgroups.blocks.AsyncRequestHandler;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.Response;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.DataReceiver;
import edu.jlime.core.transport.Streamer;
import edu.jlime.core.transport.Transport;
import edu.jlime.metrics.metric.Metrics;

public class JgroupsTransport implements AsyncRequestHandler, Transport {

	private static final long FIRST_RETRY = 5;

	private static final int MAX_RETRY = 5;

	private JChannel channel;

	private MessageDispatcher disp;

	ExecutorService handleExecutor = Executors.newFixedThreadPool(20,
			new ThreadFactory() {

				@Override
				public Thread newThread(Runnable r) {
					Thread t = Executors.defaultThreadFactory().newThread(r);
					t.setName("JGroupsRPCDispatcherHandler");
					// t.setDaemon(true);
					return t;
				}
			});

	private String id;

	private InputStream jg;

	private Logger log = Logger.getLogger(JgroupsTransport.class);

	private ArrayList<OnViewChangeListener> viewchangelisteners = new ArrayList<>();

	private RequestOptions syncOpts = RequestOptions.SYNC().setFlags(Flag.OOB)
			.setTimeout(Long.MAX_VALUE);

	private RequestOptions asyncOpts = RequestOptions.ASYNC()
			.setFlags(Flag.OOB).setTimeout(Long.MAX_VALUE);

	public JgroupsTransport(String id, InputStream jg) throws Exception {
		this.id = id;
		this.jg = jg;
		Thread vu = new Thread("View Updater") {
			public void run() {
				while (true) {
					updateView();

				}
			};
		};
		// vu.setDaemon(true);
		vu.start();
	}

	private ConcurrentLinkedDeque<View> updateQueue = new ConcurrentLinkedDeque<>();

	private DataReceiver rcvr;

	private Metrics metrics;

	protected synchronized void updateView() {
		while (updateQueue.isEmpty())
			try {
				wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		View view = updateQueue.pop();
		for (OnViewChangeListener l : viewchangelisteners) {
			l.viewChanged(view);
		}
	}

	public Address getAddress() {
		return channel.getAddress();
	}

	@Override
	public Object handle(Message msg) throws Exception {
		return null;
	}

	@Override
	public void handle(final Message msg, final Response response)
			throws Exception {
		if (log.isDebugEnabled())
			log.debug("Handling method call from " + msg.getSrc());
		handleExecutor.execute(new Runnable() {
			public void run() {
				Address origin = msg.getSrc();
				byte[] buff = msg.getBuffer();
				byte[] resp = rcvr.process(new JGroupsAddress(origin), buff);
				if (response != null)
					response.send(resp, false);
			}
		});
	}

	public void onViewChange(OnViewChangeListener list) {
		this.viewchangelisteners.add(list);
	}

	public void start() throws Exception {

		channel = new JChannel(jg);

		channel.setName(id);

		ReceiverAdapter rcv = new ReceiverAdapter() {
			@Override
			public void viewAccepted(View view) {
				super.viewAccepted(view);
				viewChanged(view);
			}
		};

		disp = new MessageDispatcher(channel, rcv, rcv, this);

		disp.asyncDispatching(true);

		channel.connect("DistributedExecutionService");
	}

	protected synchronized void viewChanged(View view) {
		updateQueue.add(view);
		notify();
	}

	public void onStop() {

	}

	@Override
	public byte[] sendSync(Peer p, byte[] marshalled) throws Exception {
		return send(p, marshalled, syncOpts);
	}

	@Override
	public void sendAsync(Peer p, byte[] marshalled) throws Exception {
		send(p, marshalled, asyncOpts);
	}

	private byte[] send(Peer p, byte[] marshalled, RequestOptions opts)
			throws Exception {
		long retryLapse = TimeUnit.MILLISECONDS.convert(FIRST_RETRY,
				TimeUnit.SECONDS);
		int retry = 0;
		while (retry < MAX_RETRY) {
			try {
				if (log.isDebugEnabled())
					log.debug("Sending JGROUPS message to " + p.getAddress());
				return disp.sendMessage(
						new Message(
								((Address) ((JGroupsAddress) p.getAddress())
										.getAddress()), marshalled), opts);
			} catch (SuspectedException e) {
				if (log.isDebugEnabled())
					log.debug("Peer "
							+ p
							+ " is suspected by JGroups. It's probably down or working really slowly. I WILL NOT RETRY.");
				throw e;
			} catch (TimeoutException e) {
				if (log.isDebugEnabled())
					log.debug("Server " + p + " timeout'ed.");
			} catch (Exception e) {
				log.error("JGroups error", e);
			}
			if (log.isDebugEnabled())
				log.debug("Retry "
						+ retry
						+ "/"
						+ MAX_RETRY
						+ " sending message to"
						+ p
						+ ". Waiting "
						+ TimeUnit.SECONDS.convert(retryLapse,
								TimeUnit.MILLISECONDS) + " sec.");
			try {
				Thread.sleep(retryLapse);
			} catch (Exception e2) {
				log.error("", e2);
			}
			retry++;
			retryLapse *= 2;
		}
		throw new Exception("After " + retry
				+ " retries, could not send data to " + p);
	}

	// Doesn't work as expected. Sends messages to all members, and then expects
	// results from a few.
	// @Override
	// public Map<String, Object> broadcast(List<Peer> copy, byte[] marshalled)
	// throws Exception {
	// List<Address> addrs = new ArrayList<Address>();
	// for (Peer peer : copy) {
	// addrs.add(((PeerJgroups) peer).getAddress());
	// }
	// Map<String, Object> resp = new HashMap<>();
	// RspList<Object> res = disp.castMessage(addrs, new Message(null,
	// marshalled), opts);
	// for (Rsp rsp : res) {
	// if (rsp.hasException())
	// resp.put(rsp.getSender().toString(), rsp.getException());
	// else
	// resp.put(rsp.getSender().toString(), rsp.getValue());
	// }
	// return resp;
	// }

	@Override
	public void registerReceiver(DataReceiver rcvr) {
		this.rcvr = rcvr;
	}

	@Override
	public void stop() {
		disp.stop();
		channel.close();
		handleExecutor.shutdown();
	}

	@Override
	public void setMetrics(Metrics metrics) {
		this.metrics = metrics;
	}

	@Override
	public Streamer getStreamer() {
		// TODO Auto-generated method stub
		return null;
	}

}
