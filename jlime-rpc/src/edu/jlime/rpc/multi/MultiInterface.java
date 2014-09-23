package edu.jlime.rpc.multi;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import edu.jlime.core.stream.RemoteInputStream;
import edu.jlime.core.stream.RemoteOutputStream;
import edu.jlime.core.transport.Address;
import edu.jlime.core.transport.Streamer;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.rpc.AddressListProvider;
import edu.jlime.rpc.Configuration;
import edu.jlime.rpc.NetworkProtocolFactory;
import edu.jlime.rpc.message.AddressType;
import edu.jlime.rpc.message.Message;
import edu.jlime.rpc.message.MessageListener;
import edu.jlime.rpc.message.MessageProcessor;
import edu.jlime.rpc.message.SocketAddress;
import edu.jlime.rpc.message.StackElement;
import edu.jlime.util.NetworkChangeListener;
import edu.jlime.util.NetworkUtils;
import edu.jlime.util.NetworkUtils.SelectedInterface;

public class MultiInterface extends MessageProcessor implements
		NetworkChangeListener, AddressListProvider, Streamer {

	private static Logger log = Logger.getLogger(MultiInterface.class);

	ConcurrentHashMap<Address, MessageProcessor> ifTable = new ConcurrentHashMap<>();

	HashMap<Address, Long> lastRcvd = new HashMap<>();

	private long max_update_time;

	private NetworkProtocolFactory factory;

	private AddressType type;

	private Metrics metrics;

	public MultiInterface(AddressType type, long max_update_time,
			NetworkProtocolFactory factory) {
		super("Multi Interface Message Processor");
		this.type = type;
		this.max_update_time = max_update_time;
		NetworkUtils.addNetworkChangeListener(this);
		this.factory = factory;
	}

	@Override
	public AddressType getType() {
		return type;
	}

	@Override
	public void onStart() throws Exception {
		createIfaces(NetworkUtils.getLocalAddress(false));
	}

	private void createIfaces(List<SelectedInterface> newIfacesList) {
		MessageListener list = new MessageListener() {
			@Override
			public void rcv(Message m, MessageProcessor origin)
					throws Exception {

				Address id = m.getFrom();
				StackElement proc = ifTable.get(id);

				if (proc == null) {
					synchronized (ifTable) {
						proc = ifTable.get(id);
						if (proc == null) {
							long t = System.currentTimeMillis();
							if (log.isDebugEnabled())
								log.debug("Updating processor for ID " + id
										+ " to " + origin);
							lastRcvd.put(id, t);
							ifTable.put(id, origin);
						}
					}

				} else {
					long curr = System.currentTimeMillis();
					Long last = lastRcvd.get(id);
					if (curr - last > max_update_time) {
						if (log.isDebugEnabled())
							log.debug("Last Time I received data from " + id
									+ " was " + (curr - last)
									+ " updating procesor " + proc
									+ " to processor " + origin);
						ifTable.put(id, origin);
					}
					lastRcvd.put(id, curr);
				}
				notifyRcvd(m);
			}

		};
		for (SelectedInterface iface : newIfacesList) {
			String addr = iface.getInet().getHostAddress();
			if (getProc(addr) == null) {
				if (log.isDebugEnabled())
					log.debug("Creating new processor for interface " + iface);

				MessageProcessor msg = factory.getProtocol(iface.getInet()
						.getHostAddress());
				try {
					msg.start();
					super.registerProcessor(iface.getInet().getHostAddress(),
							msg);
					msg.addAllMessageListener(list);

					if (metrics != null)
						msg.setMetrics(metrics);
				} catch (Exception e) {
					log.error("Could not create processor on iface " + iface, e);
				}

			}
		}
	}

	@Override
	public void send(Message msg) throws Exception {
		Address to = msg.getTo();
		if (to != null) {

			MessageProcessor proc = null;
			synchronized (ifTable) {
				proc = ifTable.get(to);
			}

			if (proc != null && !proc.isStopped()) {
				if (log.isDebugEnabled())
					log.debug("Sending Message of type " + msg.getType()
							+ " to " + to + " using processor " + proc);
				proc.queue(msg);
				return;
			}
		}

		if (log.isDebugEnabled())
			log.debug("Sending Message of type " + msg.getType()
					+ " to all processors because processor for " + to
					+ " was not found");
		sendToAllProcs(msg);
	}

	private void sendToAllProcs(Message msg) {
		for (MessageProcessor mp : super.getProcessors())
			try {
				mp.queue(msg);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}

	@Override
	public void interfacesChanged(List<SelectedInterface> added,
			List<SelectedInterface> removed) {

		createIfaces(added);
		for (SelectedInterface si : removed) {
			String addr = si.getInet().getHostAddress();
			MessageProcessor p = super.removedProc(addr);
			if (p != null)
				try {
					p.stop();
				} catch (Exception e) {
					e.printStackTrace();
				}
		}
	}

	@Override
	public List<SocketAddress> getAddresses() {
		synchronized (ifTable) {
			List<SocketAddress> addresses = new ArrayList<>();
			for (StackElement mp : getProcessors()) {
				addresses.addAll(((AddressListProvider) mp).getAddresses());
			}
			return addresses;
		}
	}

	@Override
	public void cleanupOnFailedPeer(Address peer) {
		for (MessageProcessor mp : getProcessors()) {
			mp.cleanupOnFailedPeer(peer);
		}
	}

	@Override
	public void updateAddress(Address id, List<SocketAddress> addresses) {
		for (MessageProcessor mp : super.getProcessors())
			((AddressListProvider) mp).updateAddress(id, addresses);
	}

	@Override
	public void onStop() throws Exception {
		super.onStop();
		for (MessageProcessor mp : getProcessors()) {
			if (mp != null)
				mp.stop();
		}
	}

	public static MultiInterface create(AddressType type, Configuration config,
			NetworkProtocolFactory fact) {
		return new MultiInterface(type, config.interface_max_update_time, fact);
	}

	@Override
	public RemoteInputStream getInputStream(UUID streamId, Address from) {
		for (MessageProcessor proc : getProcessors()) {
			try {
				Streamer streamer = (Streamer) proc;
				return streamer.getInputStream(streamId, from);
			} catch (Exception e) {
			}
		}
		return null;
	}

	@Override
	public RemoteOutputStream getOutputStream(UUID streamId, Address to) {
		for (MessageProcessor proc : getProcessors()) {
			try {
				Streamer streamer = (Streamer) proc;
				return streamer.getOutputStream(streamId, to);
			} catch (Exception e) {
			}
		}
		return null;
	}

	@Override
	public void setMetrics(Metrics metrics) {
		this.metrics = metrics;
		for (MessageProcessor proc : getProcessors()) {
			proc.setMetrics(metrics);
		}
	}
}
