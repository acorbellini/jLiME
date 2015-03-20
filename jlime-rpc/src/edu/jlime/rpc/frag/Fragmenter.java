package edu.jlime.rpc.frag;

import java.util.Arrays;
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

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

	public static final int FRAG_OVERHEAD = 24 + Header.HEADER;

	Object[] locks = new Object[4093];

	ConcurrentHashMap<UUID, IncompleteMessage> parts = new ConcurrentHashMap<>();

	ConcurrentHashMap<Address, HashSet<UUID>> receivedFragments = new ConcurrentHashMap<>();

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

				UUID fragID = header.getUUID();

				int offset = header.getInt();

				int messageLength = header.getInt();

				IncompleteMessage incomplete = parts.get(fragID);
				if (incomplete == null) {
					synchronized (parts) {
						incomplete = parts.get(fragID);
						if (incomplete == null) {

							HashSet<UUID> sentIDs = receivedFragments.get(from);
							if (sentIDs == null) {
								synchronized (receivedFragments) {
									sentIDs = receivedFragments.get(from);
									if (sentIDs == null) {
										sentIDs = new HashSet<>();
										receivedFragments.put(from, sentIDs);
									}
								}
							}
							if (sentIDs.contains(fragID)) {
								if (log.isDebugEnabled())
									log.info("Repeated fragment received");
								return;
							}
							sentIDs.add(fragID);

							incomplete = new IncompleteMessage(from,
									messageLength, fragID);

							parts.put(fragID, incomplete);
						}
					}
				}
				if (!incomplete.contains(offset)) {
					synchronized (incomplete) {
						if (!incomplete.contains(offset)) {
							incomplete.addPart(offset, message.getDataAsBytes());
							if (incomplete.isCompleted()) {
								parts.remove(fragID);
								notifyRcvd(Message.deEncapsulate(
										incomplete.getBuff(), from, to));
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

			UUID fragid = UUID.randomUUID();

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
				headerW.putUUID(fragid);

				// Offset
				headerW.putInt(offset);

				// Total Message Length
				headerW.putInt(data.length);

				sendNext(toSend);
			}
		}
	}

	@Override
	public void cleanupOnFailedPeer(Address addr) {
		synchronized (parts) {
			HashSet<UUID> sentIDs = receivedFragments.get(addr);
			if (sentIDs != null) {
				for (UUID id : sentIDs) {
					parts.remove(id);
				}
			}
		}
	}

	@Override
	public void setMetrics(Metrics metrics) {

	}
}
