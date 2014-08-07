package edu.jlime.rpc.bundler;

import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import edu.jlime.metrics.metric.Metrics;
import edu.jlime.rpc.message.Address;
import edu.jlime.rpc.message.Message;
import edu.jlime.rpc.message.MessageListener;
import edu.jlime.rpc.message.MessageProcessor;
import edu.jlime.rpc.message.MessageType;
import edu.jlime.rpc.message.SimpleMessageProcessor;
import edu.jlime.util.ByteBuffer;

public class MessageBundler extends SimpleMessageProcessor {

	ConcurrentHashMap<Address, Bundler> bundles = new ConcurrentHashMap<>();

	private int max_size;

	private Logger log = Logger.getLogger(MessageBundler.class);

	private Timer timer;

	public MessageBundler(MessageProcessor next, int size) {
		super(next, "Bundler");
		this.max_size = 64;
		this.timer = new Timer("Bundler Timer");
	}

	@Override
	public void start() throws Exception {
		getNext().addMessageListener(MessageType.BUNDLE, new MessageListener() {
			@Override
			public void rcv(Message defMessage, MessageProcessor origin)
					throws Exception {
				ByteBuffer reader = defMessage.getDataBuffer();
				while (reader.hasRemaining()) {
					byte[] msg = reader.getByteArray();
					try {
						notifyRcvd(Message.deEncapsulate(msg,
								defMessage.getFrom(), defMessage.getTo()));
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		});
		getNext().addSecondaryMessageListener(new MessageListener() {

			@Override
			public void rcv(Message defMessage, MessageProcessor origin)
					throws Exception {
				notifyRcvd(defMessage);
			}
		});
	}

	@Override
	public void send(Message msg) throws Exception {
		int size = msg.getSize();
		if (size + 4 > max_size) {
			if (log.isDebugEnabled())
				log.debug("Sending NOT BUNDLED Message of type "
						+ msg.getType() + " and size " + msg.getSize());
			sendNext(msg);
			return;
		}
		Address to = msg.getTo();
		if (to == null)
			to = Address.noAddr();
		Bundler bundler = bundles.get(to);
		if (bundler == null)
			synchronized (bundles) {
				bundler = bundles.get(to);
				if (bundler == null) {
					bundler = new Bundler(getNext(), max_size, to, timer);
					bundles.put(to, bundler);
				}
			}
		bundler.send(msg);

		// synchronized (bundler) {
		// int msgSize = msg.getSize();
		// if (msgSize > max_size) {
		// System.out.println("Bypassing message of size " + msgSize);
		// sendNext(msg);
		// return;
		// } else if (bundler.size() + msgSize > max_size)
		// sendBundle(to, bundler);
		//
		// bundler.putByteArray(msg.toByteArray());
		// }
	}

	// private void sendBundle(DEFAddress to, DEFByteBufferWriter bundler)
	// throws Exception {
	// System.out.println("Sending bundle of size " + bundler.size());
	//
	// DEFMessage bundle = DEFMessage.newOutDataMessage(bundler.build(),
	// MessageType.BUNDLE, to);
	// sendNext(bundle);
	// bundler.clear();
	// }

	// private DEFByteBufferWriter newBundler(final DEFAddress to) {
	// final DEFByteBufferWriter bundler = new DEFByteBufferWriter();
	//
	// return bundler;
	// }

	@Override
	public void cleanupOnFailedPeer(Address addr) {
		// synchronized (bundles) {
		Bundler b = bundles.remove(addr);
		if (b != null)
			try {
				b.stop();
			} catch (Exception e) {
				e.printStackTrace();
			}
		// }
	}

	@Override
	public void onStop() throws Exception {
		timer.cancel();
	}

	@Override
	public void setMetrics(Metrics metrics) {

	}
}
