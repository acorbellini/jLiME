package edu.jlime.rpc.frag;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import edu.jlime.core.transport.Address;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.rpc.Option;
import edu.jlime.rpc.message.Header;
import edu.jlime.rpc.message.Message;
import edu.jlime.rpc.message.MessageListener;
import edu.jlime.rpc.message.MessageProcessor;
import edu.jlime.rpc.message.MessageSimple;
import edu.jlime.rpc.message.MessageType;
import edu.jlime.rpc.message.SimpleMessageProcessor;
import edu.jlime.util.ByteBuffer;

public class Fragmenter extends SimpleMessageProcessor {

	public static final int FRAG_OVERHEAD = 12 + Header.HEADER;

	Object[] locks = new Object[4093];

	ConcurrentHashMap<Address, Map<Integer, IncompleteMessage>> parts = new ConcurrentHashMap<>();

	AtomicInteger id = new AtomicInteger(0);

	Logger log = Logger.getLogger(Fragmenter.class);

	private int maxBytes;

	public Fragmenter(MessageProcessor next, int maxBytes) {
		super(next, "Frag");

		this.maxBytes = maxBytes - FRAG_OVERHEAD;

		for (int i = 0; i < locks.length; i++) {
			locks[i] = new Object();
		}
	}

	@Override
	public void onStart() throws Exception {
		getNext().addSecondaryMessageListener(new MessageListener() {
			@Override
			public void rcv(Message message, MessageProcessor origin)
					throws Exception {
				if (log.isDebugEnabled())
					log.debug("Received msg type " + message.getType()
							+ " to be bypassed");
				notifyRcvd(message);
			}
		});

		getNext().addMessageListener(MessageType.FRAG, new MessageListener() {
			@Override
			public void rcv(Message message, MessageProcessor origin)
					throws Exception {
				ByteBuffer header = message.getHeaderBuffer();

				Address from = message.getFrom();
				Address to = message.getTo();
				// Only the first four bytes.

				int fragID = header.getInt();

				int offset = header.getInt();

				int messageLength = header.getInt();

				Map<Integer, IncompleteMessage> incompletes = parts.get(from);
				if (incompletes == null) {
					synchronized (parts) {
						incompletes = parts.get(from);
						if (incompletes == null) {
							incompletes = new ConcurrentHashMap<Integer, IncompleteMessage>();
							parts.put(from, incompletes);
						}
					}
				}

				IncompleteMessage incomplete = incompletes.get(fragID);
				if (incomplete == null) {
					synchronized (incompletes) {
						incomplete = incompletes.get(fragID);
						if (incomplete == null) {
							if (log.isDebugEnabled())
								log.debug("Received first fragment of message with id "
										+ fragID + " from " + from);

							incomplete = new IncompleteMessage(from,
									messageLength, fragID);

							incompletes.put(fragID, incomplete);
						}
					}
				}
				if (!incomplete.contains(offset)) {
					synchronized (incomplete) {
						if (!incomplete.contains(offset)) {
							if (log.isDebugEnabled())
								log.debug("Adding offset " + offset
										+ " to message with id " + fragID
										+ " from " + from);
							incomplete.addPart(offset, message.getDataAsBytes());
							if (incomplete.isCompleted()) {
								if (log.isDebugEnabled())
									log.debug("Notifying COMPLETED Message with id "
											+ fragID + " from " + from);

								parts.get(from).remove(fragID);

								notifyRcvd(Message.deEncapsulate(
										incomplete.getBuff(), from, to));
							} else {
								if (log.isDebugEnabled())
									log.debug("Remaining bytes "
											+ incomplete.remaining
											+ " for message with id " + fragID
											+ " from " + from);

							}
						}
					}
				} else if (log.isDebugEnabled())
					log.debug("Repeated");

			}
		});
	}

	@Override
	public void send(Message msg) throws Exception {
		if (msg.getSize() <= maxBytes || msg.hasOption(Option.DO_NOT_FRAGMENT)) {
			// System.out.println("Not fragmented");
			sendNext(msg);
		} else {
			byte[] data = msg.toByteArray();

			int numMsg = (int) Math.ceil(data.length / ((double) maxBytes));

			int fragid = id.getAndIncrement();

			for (int i = 0; i < numMsg; i++) {

				int remaining = data.length - i * (maxBytes);

				int offset = i * maxBytes;

				// byte[] fragData = Arrays.copyOfRange(data, offset, offset
				// + Math.min(maxBytes, remaining));

				ByteBuffer fragData = new ByteBuffer(data, offset
						+ Math.min(maxBytes, remaining), offset);

				Message toSend = new MessageSimple(
						new Header(MessageType.FRAG), fragData, msg.getFrom(),
						msg.getTo());

				ByteBuffer headerW = toSend.getHeaderBuffer();

				// Frag UUID
				headerW.putInt(fragid);

				// Offset
				headerW.putInt(offset);

				// Total Message Length
				headerW.putInt(data.length);

				if (log.isDebugEnabled())
					log.debug("Sending fragment with size " + toSend.getSize()
							+ " offset " + offset + " (" + (i + 1) + "/"
							+ numMsg + ") with id " + fragid + " to "
							+ msg.getTo());
				sendNext(toSend);
			}
		}
	}

	@Override
	public void cleanupOnFailedPeer(Address addr) {
		parts.remove(addr);
	}

	@Override
	public void setMetrics(Metrics metrics) {

	}
}
