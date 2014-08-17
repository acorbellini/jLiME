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
			public void rcv(Message defMessage, MessageProcessor origin)
					throws Exception {
				if (log.isDebugEnabled())
					log.debug("Received msg type " + defMessage.getType()
							+ " to be bypassed");
				notifyRcvd(defMessage);
			}
		});

		getNext().addMessageListener(MessageType.FRAG, new MessageListener() {
			@Override
			public void rcv(Message defMessage, MessageProcessor origin)
					throws Exception {
				Buffer header = defMessage.getHeaderBuffer();

				Address from = defMessage.getFrom();
				Address to = defMessage.getTo();
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
									defMessage.getDataAsBytes());
							if (incomplete.isCompleted()) {
								parts.remove(fragID);
								notifyRcvd(Message.deEncapsulate(
										incomplete.getBuff(), from, to));
							}
						}
					}
				} else
					System.out.println("Repeated");

				// } else {
				// byte[] msgData = data.getByteArray();
				// DEFMessage msg = DEFMessage.fromBytes(msgID, msgData,
				// (InetSocketAddress) p.getSocketAddress(),
				// (InetSocketAddress) sock.getLocalSocketAddress());
				// // log.info("Received not fragmente message of type "
				// +
				// // msg.getType()
				// // + " and size " + msgData.length);
				// notifyRcvd(msg);
				// }
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

		// else {
		// DEFByteBuffer buff = new DEFByteBuffer();
		// // ByteBuffer.allocate(data_size);
		// buff.putBoolean(false);// it isn't fragmented
		// // Most signf UUID
		// buff.putUUID(msg.getId());
		//
		// buff.putByteArray(data);
		//
		// byte[] built = buff.build();
		// try {
		// DatagramPacket dg = new DatagramPacket(built, built.length,
		// msg.getTo());
		// packetsTx.putFirst(dg);
		// } catch (InterruptedException e) {
		// e.printStackTrace();
		// }
		// }
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
