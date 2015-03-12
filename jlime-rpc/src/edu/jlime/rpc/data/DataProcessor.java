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
import edu.jlime.util.ByteBuffer;
import edu.jlime.util.PerfMeasure;
import edu.jlime.util.compression.CompressionType;
import edu.jlime.util.compression.Compressor;

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

	Compressor comp = CompressionType.SNAPPY.getComp();

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
		Message toSend = Message.newOutDataMessage(comp.compress(msg),
				MessageType.DATA, to);
		ByteBuffer headerWriter = toSend.getHeaderBuffer();
		headerWriter.putUUID(id);
		headerWriter.putInt(msg.length);
		headerWriter.putBoolean(waitForResponse);

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
			return null;
		}
		Message resp = r.getResponse();
		int originalSize = resp.getHeaderBuffer().getInt();
		return comp.uncompress(resp.getDataBuffer().build(), originalSize);
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
		// if (log.isDebugEnabled())
		// log.debug("Received RESPONSE for DATA message with id " + id
		// + " from " + message.getFrom());
		DataResponse resp = responses.get(id);
		if (resp != null)
			resp.setMsg(message);
	}

	private void processData(final Message m) {
		ByteBuffer head = m.getHeaderBuffer();
		final UUID id = head.getUUID();
		if (map.get(id) != null) {
			return;
		}

		map.put(id, true);
		// if (log.isDebugEnabled())
		// log.debug("Received DATA message with id " + id + " from "
		// + m.getFrom());

		int originalSize = head.getInt();

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
					Message toSend = Message.newOutDataMessage(
							comp.compress(resp), MessageType.RESPONSE,
							m.getFrom());
					ByteBuffer headerBuffer = toSend.getHeaderBuffer();
					headerBuffer.putUUID(this.msgID);
					headerBuffer.putInt(resp.length);
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
			DataMessage data = new DataMessage(comp.uncompress(
					m.getDataAsBytes(), originalSize), id, m.getTo(),
					m.getFrom());
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

		if (list != null)
			list.remove(id);

		responses.remove(id);
	}
}
