package edu.jlime.rpc.bundler;

import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import edu.jlime.core.transport.Address;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.rpc.message.Message;
import edu.jlime.rpc.message.MessageListener;
import edu.jlime.rpc.message.MessageProcessor;
import edu.jlime.rpc.message.MessageSimple;
import edu.jlime.rpc.message.MessageType;
import edu.jlime.rpc.message.SimpleMessageProcessor;
import edu.jlime.util.ByteBuffer;
import edu.jlime.util.RingQueue;

public class MessageBundler extends SimpleMessageProcessor {

	private static final int HEADER = 5;

	private static class Bundle {
		public Bundle(Address to) {
			this.to = to;
			this.queue = new RingQueue();
		}

		Address to;
		RingQueue queue;
	}

	ConcurrentHashMap<Address, Bundle> bundles = new ConcurrentHashMap<>();

	ArrayList<Bundle> bundleList = new ArrayList<>();

	private int max_size;

	private Logger log = Logger.getLogger(MessageBundler.class);

	private Timer timer;

	public MessageBundler(MessageProcessor next, int size) {
		super(next, "Bundler");
		this.max_size = size - HEADER;
		this.timer = new Timer("Bundler Timer");
	}

	@Override
	public void onStart() throws Exception {
		getNext().addMessageListener(MessageType.BUNDLE, new MessageListener() {
			@Override
			public void rcv(Message message, MessageProcessor origin)
					throws Exception {
				ByteBuffer reader = message.getDataBuffer();
				while (reader.hasRemaining()) {
					byte[] msg = reader.getByteArray();
					try {
						MessageSimple deEncapsulate = Message.deEncapsulate(
								msg, message.getFrom(), message.getTo());
						notifyRcvd(deEncapsulate);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		});
		getNext().addSecondaryMessageListener(new MessageListener() {

			@Override
			public void rcv(Message message, MessageProcessor origin)
					throws Exception {
				notifyRcvd(message);
			}
		});

		timer.schedule(new TimerTask() {
			@Override
			public void run() {
				for (Bundle b : bundleList) {
					Message msg = null;
					Message curr = null;
					while ((curr = (Message) b.queue.tryTakeOne()) != null) {
						if (msg == null)
							msg = Message.newEmptyOutDataMessage(
									MessageType.BUNDLE, b.to);
						if (msg.getSize() + curr.getSize() < max_size) {
							msg.getDataBuffer()
									.putByteArray(curr.toByteArray());
						}
					}
					if (msg != null) {
						try {
							sendNext(msg);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
			}
		}, 1, 1);
	}

	@Override
	public void send(Message msg) throws Exception {
		int size = msg.getSize();
		if (size > max_size) {
			if (log.isDebugEnabled())
				log.debug("Sending NOT BUNDLED Message of type "
						+ msg.getType() + " and size " + msg.getSize());
			sendNext(msg);
			return;
		}
		Address to = (Address) msg.getTo();
		if (to == null)
			to = Address.noAddr();

		Bundle b = bundles.get(to);
		if (b == null) {
			synchronized (bundles) {
				b = bundles.get(to);
				if (b == null) {
					b = new Bundle(to);
					bundles.put(to, b);
					bundleList.add(b);
				}
			}
		}
		b.queue.put(msg);
	}

	@Override
	public void cleanupOnFailedPeer(Address addr) {
		bundles.remove(addr);
		bundleList.remove(addr);
	}

	@Override
	public void onStop() throws Exception {
		timer.cancel();
	}

	@Override
	public void setMetrics(Metrics metrics) {

	}
}
