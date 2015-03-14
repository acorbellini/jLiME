package edu.jlime.rpc;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import edu.jlime.core.transport.Address;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.rpc.message.Header;
import edu.jlime.rpc.message.Message;
import edu.jlime.rpc.message.MessageListener;
import edu.jlime.rpc.message.MessageProcessor;
import edu.jlime.rpc.message.MessageType;
import edu.jlime.rpc.message.SimpleMessageProcessor;
import edu.jlime.util.ByteBuffer;
import edu.jlime.util.DataTypeUtils;

public class UDPResender extends SimpleMessageProcessor {

	public static class Nack {
		public Nack(Long msg, long time, Address from) {
			this.seq = msg;
			this.lasttime = time;
			this.from = from;
		}

		Long seq;
		long lasttime;
		volatile boolean rcvd = false;
		protected Address from;
	}

	public static class SendMsg {
		private UUID id;

		public SendMsg(Message msg, UUID id, long time) {
			this.msg = msg;
			this.lasttime = time;
			this.id = id;
		}

		Message msg;
		long lasttime;
		volatile boolean rcvd = false;
	}

	public static final int HEADER = 16 + Header.HEADER;

	ConcurrentHashMap<UUID, SendMsg> sent = new ConcurrentHashMap<>();

	Timer t = new Timer("Resender Timer");

	private Configuration config;

	protected ConcurrentHashMap<Address, List<UUID>> acks = new ConcurrentHashMap<>();

	private int max_size;

	public UDPResender(MessageProcessor next, Configuration config, int max_size) {
		super(next, "UDP resender");
		this.max_size = max_size;
		next.addMessageListener(MessageType.ACK, new MessageListener() {

			@Override
			public void rcv(Message msg, MessageProcessor origin)
					throws Exception {

				ByteBuffer headerBuffer = msg.getHeaderBuffer();

				receivedAckBuffer(headerBuffer);
			}
		});

		this.config = config;

		next.addSecondaryMessageListener(new MessageListener() {

			@Override
			public void rcv(Message msg, MessageProcessor origin)
					throws Exception {
				notifyRcvd(msg);
			}
		});

		next.addMessageListener(MessageType.ACK_SEQ, new MessageListener() {

			@Override
			public void rcv(Message msg, MessageProcessor origin)
					throws Exception {

				ByteBuffer headerBuffer = msg.getHeaderBuffer();

				UUID id = headerBuffer.getUUID();

				Address from = msg.getFrom();
				List<UUID> list = acks.get(from);
				if (list == null) {
					synchronized (acks) {
						list = acks.get(from);
						if (list == null) {
							list = new ArrayList<UUID>();
							acks.put(from, list);
						}
					}
				}

				synchronized (list) {
					list.add(id);
				}

				receivedAckBuffer(headerBuffer);

				Message encap = Message.deEncapsulate(msg.getDataAsBytes(),
						msg.getFrom(), msg.getTo());
				notifyRcvd(encap);
			}

		});

		t.schedule(new TimerTask() {
			@Override
			public void run() {
				for (SendMsg v : sent.values()) {
					if ((System.currentTimeMillis() - v.lasttime) > UDPResender.this.config.ack_delay) {
						try {
							v.lasttime = System.currentTimeMillis();
							if (!v.rcvd)
								send0(v.msg, v.id);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
			}
		}, config.retransmit_delay, config.retransmit_delay);

		t.schedule(new TimerTask() {

			@Override
			public void run() {
				for (Entry<Address, List<UUID>> e : acks.entrySet()) {
					List<UUID> value = e.getValue();
					if (value != null)
						synchronized (value) {
							while (!value.isEmpty()) {
								Message ack = Message.newEmptyOutDataMessage(
										MessageType.ACK, e.getKey());
								attachAcks(ack, value);
								try {
									sendNext(ack);
								} catch (Exception e1) {
									e1.printStackTrace();
								}
							}
						}
				}
			}
		}, config.ack_delay, config.ack_delay);
	}

	@Override
	public void setMetrics(Metrics metrics) {
	}

	@Override
	public void send(Message msg) throws Exception {
		UUID id = UUID.randomUUID();
		sent.put(id, new SendMsg(msg, id, System.currentTimeMillis()));
		send0(msg, id);
	}

	private void receivedAckBuffer(ByteBuffer headerBuffer) {
		if (headerBuffer.hasRemaining()) {
			int acksCount = headerBuffer.getInt();
			for (int i = 0; i < acksCount; i++) {
				SendMsg s = sent.remove(headerBuffer.getUUID());
				if (s != null)
					s.rcvd = true;
			}
		}
	}

	private void send0(Message msg, UUID id) throws Exception {

		Message acked = Message.encapsulate(msg, MessageType.ACK_SEQ,
				msg.getFrom(), msg.getTo());
		ByteBuffer headerBuffer = acked.getHeaderBuffer();
		headerBuffer.putUUID(id);

		List<UUID> list = acks.get(msg.getTo());

		attachAcks(acked, list);

		sendNext(acked);

	}

	private void attachAcks(Message msg, List<UUID> list) {
		if (list == null)
			return;
		ByteBuffer buff = msg.getHeaderBuffer();
		int size = msg.getSize();
		int diff = (max_size - 4) - size;
		int count = diff / 16;
		if (count > 0)
			synchronized (list) {
				if (!list.isEmpty()) {
					buff.putInt(Math.min(count, list.size()));
					Iterator<UUID> it = list.iterator();
					for (int i = 0; i < count && it.hasNext(); i++) {
						UUID uuid = (UUID) it.next();
						buff.putUUID(uuid);
						it.remove();
					}

				}
			}
	}

	@Override
	protected void onStop() throws Exception {
		t.cancel();
	}

}
