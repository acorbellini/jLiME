package edu.jlime.rpc.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

import org.apache.log4j.Logger;

import edu.jlime.core.cache.jLiMELRUMap;
import edu.jlime.core.transport.Address;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.rpc.message.Message;
import edu.jlime.rpc.message.MessageListener;
import edu.jlime.rpc.message.MessageProcessor;
import edu.jlime.rpc.message.MessageType;
import edu.jlime.rpc.message.SimpleMessageProcessor;
import edu.jlime.util.Buffer;

public class DataProcessor extends SimpleMessageProcessor implements
		DataProvider {

	private jLiMELRUMap<UUID, Boolean> map = new jLiMELRUMap<>(100);

	private Logger log = Logger.getLogger(DataProcessor.class);

	private UUID localID;

	private List<DataListener> listeners = new ArrayList<>();

	private HashMap<UUID, Object> waitingResponse = new HashMap<>();

	private HashMap<UUID, Message> responses = new HashMap<>();

	private HashMap<Address, HashSet<UUID>> calls = new HashMap<>();

	private Metrics metrics;

	public static class DataMessage {

		private Address from;

		private byte[] data;

		public DataMessage(byte[] data, UUID msgID, Address to, Address from) {
			this.setData(data);
			this.from = from;
		}

		public byte[] getData() {
			return data;
		}

		public void setData(byte[] data) {
			this.data = data;
		}

		public Address getFrom() {
			return from;
		}

		public void setFrom(Address from) {
			this.from = from;
		}
	}

	public DataProcessor(MessageProcessor next) {
		super(next, "Data");
	}

	@Override
	public void onStart() throws Exception {
		getNext().addMessageListener(MessageType.DATA, new MessageListener() {
			@Override
			public void rcv(final Message m, MessageProcessor origin)
					throws Exception {
				processData(m);
			}

		});
		getNext().addMessageListener(MessageType.RESPONSE,
				new MessageListener() {
					@Override
					public void rcv(Message message, MessageProcessor origin) {
						processResponse(message);
					}
				});
	}

	@Override
	public void send(Message msg) throws Exception {
	}

	@Override
	public void addDataListener(DataListener list) {
		this.listeners.add(list);
	}

	@Override
	public byte[] sendData(byte[] msg, Address to, boolean waitForResponse)
			throws Exception {
		Object lock = new Object();
		UUID id = UUID.randomUUID();

		Message toSend = Message.newOutDataMessage(msg, MessageType.DATA, to);
		Buffer headerWriter = toSend.getHeaderBuffer();
		headerWriter.putUUID(id);
		headerWriter.putBoolean(waitForResponse);
		if (log.isDebugEnabled())
			log.debug("Sending DATA message with id " + id + " to " + to);

		if (waitForResponse) {
			synchronized (waitingResponse) {
				waitingResponse.put(id, lock);
				HashSet<UUID> ids = calls.get(to);
				if (ids == null) {
					ids = new HashSet<>();
					calls.put(to, ids);
				}
				ids.add(id);
			}
		}

		sendNext(toSend);

		if (!waitForResponse) {
			if (log.isDebugEnabled())
				log.debug("DATA message " + id + " to " + to
						+ " does NOT require RESPONSE.");
			return null;
		} else {
			if (log.isDebugEnabled())
				log.debug("Waiting for response for DATA message with id " + id
						+ " to " + to);
			synchronized (lock) {
				while (!responses.containsKey(id)) {
					lock.wait(5000);
					if (log.isDebugEnabled())
						log.debug("Still waiting for response for DATA message with id "
								+ id + " to " + to);
				}
			}

			if (log.isDebugEnabled())
				log.debug("Response rcvd for DATA message with id " + id
						+ " to " + to);
			Message resp = responses.remove(id);
			return resp.getDataBuffer().build();
		}
	}

	@Override
	public void cleanupOnFailedPeer(Address addr) {
		synchronized (waitingResponse) {
			HashSet<UUID> ids = calls.get(addr);
			if (ids != null) {
				for (UUID uuid : ids) {
					Object lock = waitingResponse.remove(uuid);
					if (lock != null) {
						synchronized (lock) {
							responses.put(uuid, Message.newEmptyOutDataMessage(
									MessageType.DATA, new Address(uuid)));
							lock.notifyAll();
						}
					}
				}
			}
		}
	}

	private void processResponse(Message message) {
		UUID id = message.getHeaderBuffer().getUUID();
		if (log.isDebugEnabled())
			log.debug("Received RESPONSE for DATA message with id " + id
					+ " from " + message.getFrom());

		synchronized (waitingResponse) {
			Object lock = waitingResponse.get(id);

			HashSet<UUID> ids = calls.get(message.getFrom());

			if (ids != null)
				ids.remove(id);
			else
				log.warn("Ids table does not contain " + message.getFrom());

			if (lock != null) {
				waitingResponse.remove(id);
				synchronized (lock) {
					responses.put(id, message);
					lock.notifyAll();
				}
			} else
				log.warn("There is no lock for message " + id);

		}
	}

	private void processData(final Message m) {
		Buffer head = m.getHeaderBuffer();
		UUID id = head.getUUID();
		if (map.get(id) != null) {
			// if (log.isDebugEnabled())
			log.info("DISCARDING recently received a DATA message with id "
					+ id + " from " + m.getFrom() + ".");
			return;
		}

		map.put(id, true);
		if (log.isDebugEnabled())
			log.debug("Received DATA message with id " + id + " from "
					+ m.getFrom());
		boolean requiresResponse = head.getBoolean();
		Response resp = null;
		if (requiresResponse) {
			resp = new Response(id) {
				@Override
				public void sendResponse(byte[] resp) throws Exception {
					if (log.isDebugEnabled())
						log.debug("Sending RESPONSE message to " + m.getFrom());
					Message toSend = Message.newOutDataMessage(resp,
							MessageType.RESPONSE, m.getFrom());
					toSend.getHeaderBuffer().putUUID(this.msgID);
					sendNext(toSend);
				}
			};
		}
		if (log.isDebugEnabled())
			log.debug("Curr listeners " + listeners.size());
		for (DataListener l : listeners) {
			DataMessage data = new DataMessage(m.getDataAsBytes(), id,
					m.getTo(), m.getFrom());
			if (log.isDebugEnabled())
				log.debug("Sending data message to listener.");
			l.messageReceived(data, resp);
		}
	}

	@Override
	public void setMetrics(Metrics metrics) {
		this.metrics = metrics;
	}
}
