package edu.jlime.rpc.frag;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.UUID;

import org.apache.log4j.Logger;

import edu.jlime.core.transport.Address;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.rpc.Option;
import edu.jlime.rpc.message.Message;
import edu.jlime.rpc.message.MessageListener;
import edu.jlime.rpc.message.MessageProcessor;
import edu.jlime.rpc.message.MessageType;
import edu.jlime.rpc.message.SimpleMessageProcessor;
import edu.jlime.util.Buffer;

public class Fragmenter extends SimpleMessageProcessor {

	HashMap<UUID, IncompleteMessage> parts = new HashMap<>();

	HashMap<Address, HashSet<UUID>> sent = new HashMap<>();

	Logger log = Logger.getLogger(Fragmenter.class);

	private int maxBytes;

	public Fragmenter(MessageProcessor next, int maxBytes) {
		super(next, "Frag");
		this.maxBytes = maxBytes;
	}

	@Override
	public void start() throws Exception {
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
				Buffer header = message.getHeaderBuffer();

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
							incomplete = new IncompleteMessage(messageLength);
							parts.put(fragID, incomplete);
							HashSet<UUID> sentIDs = sent.get(to);
							if (sentIDs == null) {
								sentIDs = new HashSet<>();
								sent.put(to, sentIDs);
							}
							sentIDs.add(fragID);
						}
					}
				}
				if (!incomplete.contains(offset)) {
					synchronized (incomplete) {
						if (!incomplete.contains(offset)) {
							incomplete.addPart(offset,
									message.getDataAsBytes());
							if (incomplete.isCompleted()) {
								parts.remove(fragID);
								notifyRcvd(Message.deEncapsulate(
										incomplete.getBuff(), from, to));
							}
						}
					}
				} else
					System.out.println("Repeated");

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

				byte[] fragData = Arrays.copyOfRange(data, offset, offset
						+ Math.min(maxBytes, remaining));

				Message toSend = Message.newOutDataMessage(fragData,
						MessageType.FRAG, msg.getTo());

				Buffer headerW = toSend.getHeaderBuffer();

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
			HashSet<UUID> sentIDs = sent.get(addr);
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
