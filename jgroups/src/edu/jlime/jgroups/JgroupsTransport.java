package edu.jlime.jgroups;

import java.io.InputStream;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.Message.Flag;
import org.jgroups.SuspectedException;
import org.jgroups.TimeoutException;
import org.jgroups.blocks.AsyncRequestHandler;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.Response;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.cluster.PeerFilter;
import edu.jlime.core.transport.Streamer;
import edu.jlime.core.transport.Transport;

public class JgroupsTransport extends Transport implements AsyncRequestHandler {

	private static final long FIRST_RETRY = 5;

	private static final int MAX_RETRY = 5;

	private MessageDispatcher disp;

	private HashMap<Peer, Address> jgroupsaddr = new HashMap<>();

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

	private RequestOptions syncOpts = RequestOptions.SYNC().setFlags(Flag.OOB)
			.setTimeout(Long.MAX_VALUE);

	private RequestOptions asyncOpts = RequestOptions.ASYNC()
			.setFlags(Flag.OOB).setTimeout(Long.MAX_VALUE);
	HashMap<edu.jlime.core.transport.Address, Address> addrMap = new HashMap<>();

	private JgroupsMembership member;

	public JgroupsTransport(Peer local, PeerFilter filter,
			MessageDispatcher disp, JgroupsMembership member, Streamer s)
			throws Exception {
		super(local, filter, member, member, s);
		this.member = member;
		disp.setRequestHandler(this);
		this.disp = disp;
		addrMap.put(local.getAddress(), disp.getChannel().getAddress());

	}

	public Address getAddress() {
		return disp.getChannel().getAddress();
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
				byte[] resp = JgroupsTransport.super.callTransportListener(
						member.getAddress(origin), buff);
				if (response != null)
					response.send(resp, false);
			}
		});
	}

	public void start() throws Exception {

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
						new Message(member.getJgroupsAddress(p.getAddress()),
								marshalled), opts);
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
	public void stop() {
		disp.stop();
		disp.getChannel().close();
		handleExecutor.shutdown();
	}

	@Override
	protected void onFailedPeer(Peer peer) {

	}

	@Override
	public String getRealAddress() {
		// TODO Auto-generated method stub
		return null;
	}

}
