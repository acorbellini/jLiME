package edu.jlime.rpc.bundler;

import java.util.Arrays;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import edu.jlime.core.transport.Address;
import edu.jlime.rpc.message.Message;
import edu.jlime.rpc.message.MessageProcessor;
import edu.jlime.rpc.message.MessageType;
import edu.jlime.util.DataTypeUtils;

public class Bundler {

	private byte[] bundle = null;

	private int pos = 0;

	private Address to;

	private Timer t;

	private int max_size;

	private Logger log = Logger.getLogger(Bundler.class);

	private MessageProcessor next;

	public Bundler(MessageProcessor next, int max_size, Address addr, Timer t) {
		this.max_size = max_size;
		this.bundle = new byte[max_size];
		this.to = addr;
		this.t = t;
		this.next = next;
		t.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
				try {
					sendBundle();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

		}, 1, 1);
	}

	protected synchronized void sendBundle() throws Exception {
		if (log.isDebugEnabled())
			log.debug("Waiting for write to finish.");

		if (pos == 0) {
			if (log.isDebugEnabled())
				log.debug("Bundle was empty.");
			return;
		}

		if (log.isDebugEnabled())
			log.debug("Sending bundle of size " + pos + ".");

		Message msg = Message.newOutDataMessage(
				Arrays.copyOfRange(bundle, 0, pos), MessageType.BUNDLE, to);
		next.send(msg);

		bundle = new byte[max_size];
		pos = 0;

	}

	public void stop() throws Exception {
		t.cancel();
	}

	public synchronized void send(Message msg) throws Exception {
		int msgSize = msg.getSize();
		if (log.isDebugEnabled())
			log.debug("Bundling message of type " + msg.getType()
					+ " and size " + msg.getSize());

		if (msgSize + 4 > bundle.length) {
			next.send(msg);
			return;
		}
		byte[] msgAsBytes = msg.toByteArray();
		if (msgAsBytes.length + 4 + pos >= bundle.length)
			sendBundle();

		System.arraycopy(DataTypeUtils.intToByteArray(msgAsBytes.length), 0,
				bundle, pos, 4);
		System.arraycopy(msgAsBytes, 0, bundle, pos + 4, msgAsBytes.length);

		pos += msgAsBytes.length + 4;
	}
}
