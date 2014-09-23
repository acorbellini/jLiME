package edu.jlime.rpc.data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

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

	private HashMap<Address, Set<UUID>> waiting = new HashMap<>();

	private List<DataListener> listeners = new ArrayList<>();

	// private HashMap<UUID, Message> responses = new HashMap<>();

	private ConcurrentHashMap<UUID, DataResponse> responses = new ConcurrentHashMap<>();

	private Metrics metrics;

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

		UUID id = UUID.randomUUID();
		Message toSend = Message.newOutDataMessage(msg, MessageType.DATA, to);
		Buffer headerWriter = toSend.getHeaderBuffer();
		headerWriter.putUUID(id);
		headerWriter.putBoolean(waitForResponse);

		// if (log.isDebugEnabled())
		// log.debug("Sending DATA message with id " + id + " to " + to);

		// new Exception().printStackTrace();

		// PerfMeasure.takeTime("data", false);

		DataResponse r = null;
		if (waitForResponse) {
			r = new DataResponse(this, to, id);
			responses.put(id, r);

			Set<UUID> list = waiting.get(to);
			if (list == null)
				synchronized (waiting) {
					list = waiting.get(to);
					if (list == null) {
						list = Collections
								.newSetFromMap(new ConcurrentHashMap<UUID, Boolean>());
						waiting.put(to, list);
					}
				}
			list.add(id);
		}

		sendNext(toSend);

		if (r == null) {
			// if (log.isDebugEnabled())
			// log.debug("DATA message " + id + " to " + to
			// + " does NOT require RESPONSE.");
			return null;
		}
		// if (log.isDebugEnabled())
		// log.debug("Waiting for response for DATA message with id " + id
		// + " to " + to);
		Message resp = r.getResponse();

		// synchronized (responses) {
		// while (!responses.containsKey(id)) {
		// responses.wait(1000);
		// if (log.isDebugEnabled())
		// log.debug("Still waiting for response for DATA message with id "
		// + id + " to " + to + " responses : " + responses);
		// }
		// resp = responses.remove(id);
		// synchronized (waiting) {
		// Set<UUID> list = waiting.get(to);
		// if (list != null) {
		// list.remove(id);
		// if (list.isEmpty())
		// waiting.remove(to);
		// }
		// }
		// }

		// if (log.isDebugEnabled())
		// log.debug("Response rcvd for DATA message with id " + id + " to "
		// + to);
		return resp.getDataBuffer().build();
	}

	@Override
	public void cleanupOnFailedPeer(Address addr) {
		synchronized (waiting) {
			Set<UUID> list = waiting.get(addr);
			if (list == null)
				return;
			synchronized (responses) {
				for (UUID uuid : list)
					responses.get(uuid).setMsg(null);
			}
			waiting.remove(addr);
		}
	}

	private void processResponse(Message message) {
		UUID id = message.getHeaderBuffer().getUUID();
		if (log.isDebugEnabled())
			log.debug("Received RESPONSE for DATA message with id " + id
					+ " from " + message.getFrom());
		DataResponse dataResponse = responses.get(id);
		if (dataResponse == null)
			System.out.println("Is null.");
		dataResponse.setMsg(message);
	}

	private void processData(final Message m) {
		Buffer head = m.getHeaderBuffer();
		final UUID id = head.getUUID();
		if (map.get(id) != null) {
			// if (log.isDebugEnabled())
			// log.info("DISCARDING recently received a DATA message with id "
			// + id + " from " + m.getFrom() + ".");
			return;
		}

		map.put(id, true);
		// if (log.isDebugEnabled())
		// log.debug("Received DATA message with id " + id + " from "
		// + m.getFrom());
		boolean requiresResponse = head.getBoolean();
		Response resp = null;
		if (requiresResponse) {
			resp = new Response(id) {
				@Override
				public void sendResponse(byte[] resp) throws Exception {
					//
					// if (log.isDebugEnabled())
					// log.debug("Preparing RESPONSE for message id " + id
					// + " to " + m.getFrom());
					Message toSend = Message.newOutDataMessage(resp,
							MessageType.RESPONSE, m.getFrom());
					toSend.getHeaderBuffer().putUUID(this.msgID);
					// if (log.isDebugEnabled())
					// log.debug("Sending RESPONSE for message id " + id
					// + " to " + m.getFrom());
					sendNext(toSend);
				}
			};
		}
		// if (log.isDebugEnabled())
		// log.debug("Curr listeners " + listeners.size());
		for (DataListener l : listeners) {
			DataMessage data = new DataMessage(m.getDataAsBytes(), id,
					m.getTo(), m.getFrom());
			// if (log.isDebugEnabled())
			// log.debug("Sending data message to listener.");
			l.messageReceived(data, resp);
		}
	}

	@Override
	public void setMetrics(Metrics metrics) {
		this.metrics = metrics;
	}

	public void removeResponse(Address addr, UUID id) {
		Set<UUID> list = waiting.get(addr);
		list.remove(id);
		responses.remove(id);
	}
}
