package edu.jlime.rpc.data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import edu.jlime.core.transport.Address;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.rpc.NetworkConfiguration;
import edu.jlime.rpc.message.Message;
import edu.jlime.rpc.message.MessageListener;
import edu.jlime.rpc.message.MessageProcessor;
import edu.jlime.rpc.message.MessageType;
import edu.jlime.rpc.message.SimpleMessageProcessor;
import edu.jlime.util.ByteBuffer;
import edu.jlime.util.compression.CompressionType;
import edu.jlime.util.compression.Compressor;

public class DataProcessor extends SimpleMessageProcessor implements
		DataProvider {

	// private jLiMELRUMap<Integer, Boolean> map = new jLiMELRUMap<>(100);

	private Logger log = Logger.getLogger(DataProcessor.class);

	private UUID localID;

	private HashMap<Address, Set<Integer>> waiting = new HashMap<>();

	private List<DataListener> listeners = new ArrayList<>();

	// private HashMap<UUID, Message> responses = new HashMap<>();

	private ConcurrentHashMap<Integer, DataResponse> responses = new ConcurrentHashMap<>();

	private Metrics metrics;

	Compressor comp;

	private AtomicInteger idCount = new AtomicInteger(0);

	public DataProcessor(MessageProcessor next, NetworkConfiguration config) {
		super(next, "Data");
		this.comp = null;
		try {
			this.comp = CompressionType.valueOf(config.compression).getComp();
		} catch (Exception e) {
		}
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
		// UUID id = UUID.randomUUID();
		int id = idCount.getAndIncrement();
		byte[] compressed = msg;
		if (comp != null)
			compressed = comp.compress(compressed);

		Message toSend = Message.newOutDataMessage(compressed,
				MessageType.DATA, to);
		ByteBuffer headerWriter = toSend.getHeaderBuffer();
		headerWriter.putInt(id);
		headerWriter.putBoolean(waitForResponse);
		if (comp != null)
			headerWriter.putInt(msg.length);

		DataResponse r = null;
		if (waitForResponse) {
			// log.info("Setting DataReponse object for message id " + id);
			r = new DataResponse(this, to, id);
			responses.put(id, r);

			Set<Integer> list = waiting.get(to);
			if (list == null)
				synchronized (waiting) {
					list = waiting.get(to);
					if (list == null) {
						list = Collections
								.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());
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
		byte[] build = resp.getDataBuffer().build();
		if (comp != null) {
			int originalSize = resp.getHeaderBuffer().getInt();
			build = comp.uncompress(build, originalSize);
		}
		return build;
	}

	@Override
	public void cleanupOnFailedPeer(Address addr) {
		synchronized (waiting) {
			Set<Integer> list = waiting.get(addr);
			if (list == null)
				return;
			synchronized (responses) {
				for (Integer uuid : list)
					responses.get(uuid).setMsg(null);
			}
			waiting.remove(addr);
		}
	}

	private void processResponse(Message message) {
		int id = message.getHeaderBuffer().getInt();
		// if (log.isDebugEnabled())
		// log.info("Received RESPONSE for DATA message with id " + id +
		// " from "
		// + message.getFrom());
		DataResponse resp = responses.get(id);
		if (resp != null)
			resp.setMsg(message);
		// else {
		// log.info("DataReponse object is null for message id " + id);
		// }
	}

	private void processData(final Message m) {
		ByteBuffer head = m.getHeaderBuffer();
		final int id = head.getInt();
		// if (map.get(id) != null) {
		// return;
		// }
		//
		// map.put(id, true);
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
					// log.info("Preparing RESPONSE for message id " + id +
					// " to "
					// + m.getFrom());
					byte[] compress = resp;
					if (comp != null)
						compress = comp.compress(resp);
					Message toSend = Message.newOutDataMessage(compress,
							MessageType.RESPONSE, m.getFrom());
					ByteBuffer headerBuffer = toSend.getHeaderBuffer();
					headerBuffer.putInt(this.msgID);
					if (comp != null)
						headerBuffer.putInt(resp.length);
					// if (log.isDebugEnabled())
					// log.info("Sending RESPONSE for message id " + id + " to "
					// + m.getFrom());
					sendNext(toSend);
				}
			};
		}
		// if (log.isDebugEnabled())
		// log.debug("Curr listeners " + listeners.size());
		for (DataListener l : listeners) {
			byte[] uncompress = m.getDataAsBytes();

			if (comp != null) {
				int originalSize = head.getInt();
				uncompress = comp.uncompress(uncompress, originalSize);
			}

			DataMessage data = new DataMessage(uncompress, id, m.getTo(),
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

	public void removeResponse(Address addr, int id) {
		Set<Integer> list = waiting.get(addr);

		if (list != null)
			list.remove(id);

		responses.remove(id);
	}
}
