package edu.jlime.rpc.discovery;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.log4j.Logger;

import edu.jlime.core.transport.Address;
import edu.jlime.core.transport.DiscoveryListener;
import edu.jlime.core.transport.DiscoveryProvider;
import edu.jlime.rpc.AddressListProvider;
import edu.jlime.rpc.Configuration;
import edu.jlime.rpc.message.AddressType;
import edu.jlime.rpc.message.Message;
import edu.jlime.rpc.message.MessageListener;
import edu.jlime.rpc.message.MessageProcessor;
import edu.jlime.rpc.message.MessageType;
import edu.jlime.rpc.message.SocketAddress;
import edu.jlime.rpc.message.StackElement;
import edu.jlime.util.NetworkChangeListener;
import edu.jlime.util.NetworkUtils;
import edu.jlime.util.NetworkUtils.SelectedInterface;

public abstract class Discovery implements DiscoveryProvider, StackElement {

	private Logger log = Logger.getLogger(Discovery.class);

	protected List<DiscoveryListener> listeners = Collections
			.synchronizedList(new ArrayList<DiscoveryListener>());

	protected Configuration config;

	protected MessageProcessor discoveryInit;

	protected MessageProcessor discoveryData;

	private HashMap<AddressType, AddressListProvider> addressProviders = new HashMap<>();

	private Map<String, String> discAdditionData = new HashMap<>();

	private Address localID;

	private String localName;

	public Discovery(Address localID, String name, Configuration config,
			MessageProcessor discoveryInit, MessageProcessor discoveryData) {
		this.localID = localID;
		this.localName = name;
		this.config = config;
		this.discoveryInit = discoveryInit;
		this.discoveryData = discoveryData;
	}

	@Override
	public void start() throws Exception {
		discoveryData.addMessageListener(MessageType.DISCOVERY_CONFIRM,
				new MessageListener() {
					@Override
					public void rcv(Message message, MessageProcessor origin)
							throws Exception {
						if (log.isDebugEnabled())
							log.debug("Discovery Confirm received from "
									+ message.getFrom());
						DiscoveryMessage disco = DiscoveryMessage
								.fromMessage(message);

						notifyAddressList(disco.getId(), disco.getAddresses());

						discoveryMessageReceived(message.getFrom(),
								disco.getName(), disco.getAdditional());
					}
				});
		discoveryData.addMessageListener(MessageType.DISCOVERY_RESPONSE,
				new MessageListener() {
					@Override
					public void rcv(Message m, MessageProcessor origin)
							throws Exception {

						DiscoveryMessage disco = DiscoveryMessage
								.fromMessage(m);

						if (log.isDebugEnabled())
							log.debug("Discovery Response received from "
									+ m.getFrom() + " with addresses "
									+ disco.getAddresses());

						notifyAddressList(disco.getId(), disco.getAddresses());

						discoveryMessageReceived(m.getFrom(), disco.getName(),
								disco.getAdditional());

						Message confirm = newDiscoveryConfirmMessage();
						confirm.setTo(m.getFrom());
						discoveryData.queue(confirm);

					}
				});

		discoveryInit.addMessageListener(MessageType.DISCOVERY,
				new MessageListener() {
					@Override
					public void rcv(Message m, MessageProcessor origin)
							throws Exception {
						DiscoveryMessage disco = DiscoveryMessage
								.fromMessage(m);

						if (new Address(disco.getId()).equals(localID))
							return;

						if (log.isDebugEnabled())
							log.debug("Discovery Message received from "
									+ m.getFrom() + " with addresses "
									+ disco.getAddresses());

						notifyAddressList(disco.getId(), disco.getAddresses());

						for (SocketAddress sock : disco.getAddresses()) {
							Message response = newDiscoveryResponseMessage();
							response.setTo(new Address(disco.getId()));
							response.setInetSocketAddress(sock);
							discoveryData.queue(response);
						}
					}

				});

		startDiscovery(NetworkUtils.getLocalAddress(false));

		NetworkUtils.addNetworkChangeListener(new NetworkChangeListener() {
			@Override
			public void interfacesChanged(List<SelectedInterface> added,
					List<SelectedInterface> removed) {
				log.info("Network Changed!! Restarting Discovery.");
				// startDiscovery(added);
			}
		});
	}

	protected void notifyAddressList(UUID id, List<SocketAddress> addresses) {
		for (Entry<AddressType, AddressListProvider> alul : addressProviders
				.entrySet()) {
			ArrayList<SocketAddress> byType = new ArrayList<>();
			for (SocketAddress socketAddress : addresses) {
				if (socketAddress.getType().equals(alul.getKey()))
					byType.add(socketAddress);
			}
			alul.getValue().updateAddress(new Address(id), byType);
		}
	}

	@Override
	public void addListener(DiscoveryListener l) {
		this.listeners.add(l);
	}

	protected Message newDiscoveryConfirmMessage() {
		return newDiscoveryMessage(MessageType.DISCOVERY_CONFIRM);
	}

	protected Message newDiscoveryMessage() {
		return newDiscoveryMessage(MessageType.DISCOVERY);
	}

	protected Message newDiscoveryResponseMessage() {
		return newDiscoveryMessage(MessageType.DISCOVERY_RESPONSE);
	}

	@Override
	public void putData(Map<String, String> dataMap) {
		this.discAdditionData.putAll(dataMap);
	}

	private Message newDiscoveryMessage(MessageType t) {
		List<SocketAddress> addresses = new ArrayList<>();
		for (AddressListProvider alp : addressProviders.values())
			addresses.addAll(alp.getAddresses());

		Message discoMsg = DiscoveryMessage.createNew(t, localID, localName,
				discAdditionData, addresses);

		return discoMsg;
	}

	public void addAddressListProvider(AddressListProvider alp) {
		addressProviders.put(alp.getType(), alp);
	}

	private void discoveryMessageReceived(Address from, String name,
			Map<String, String> additional) throws Exception {
		for (DiscoveryListener l : new ArrayList<>(listeners)) {
			try {
				l.memberMessage(from, name, additional);
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void cleanupOnFailedPeer(Address address) {
	}

	@Override
	public void stop() throws Exception {
	}

	protected abstract void startDiscovery(List<SelectedInterface> added);

}