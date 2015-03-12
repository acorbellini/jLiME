package edu.jlime.rpc.bundler;

import java.util.HashMap;
import java.util.Timer;

import org.apache.log4j.Logger;

import edu.jlime.core.transport.Address;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.rpc.message.Message;
import edu.jlime.rpc.message.MessageListener;
import edu.jlime.rpc.message.MessageProcessor;
import edu.jlime.rpc.message.MessageType;
import edu.jlime.rpc.message.SimpleMessageProcessor;
import edu.jlime.util.ByteBuffer;

public class MessageBundler extends SimpleMessageProcessor {

	HashMap<Address, Bundler> bundles = new HashMap<>();

	private int max_size;

	private Logger log = Logger.getLogger(MessageBundler.class);

	private Timer timer;

	public MessageBundler(MessageProcessor next, int size) {
		super(next, "Bundler");
		this.max_size = 64;
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
						notifyRcvd(Message.deEncapsulate(msg,
								message.getFrom(), message.getTo()));
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
		Address to = (Address) msg.getTo();
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
	}

	@Override
	public void cleanupOnFailedPeer(Address addr) {
		Bundler b = null;
		synchronized (bundles) {
			b = bundles.remove(addr);
		}
		if (b != null)
			try {
				b.stop();
			} catch (Exception e) {
				e.printStackTrace();
			}
	}

	@Override
	public void onStop() throws Exception {
		timer.cancel();
	}

	@Override
	public void setMetrics(Metrics metrics) {

	}
}
