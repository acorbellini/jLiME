package edu.jlime.rpc;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.apache.log4j.Logger;

import edu.jlime.core.transport.Address;
import edu.jlime.core.transport.FailureListener;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.rpc.message.AddressType;
import edu.jlime.rpc.message.Message;
import edu.jlime.rpc.message.MessageProcessor;
import edu.jlime.rpc.message.SocketAddress;

public class UDPNIO extends MessageProcessor implements AddressListProvider,
		FailureListener {

	public static final int HEADER = 32;

	private ExecutorService exec = Executors.newFixedThreadPool(64,
			new ThreadFactory() {

				@Override
				public Thread newThread(Runnable r) {
					Thread t = Executors.defaultThreadFactory().newThread(r);
					t.setName("UDP NIO Worker Thread");
					return t;
				}
			});

	ByteBuffer readbuffer;
	// ByteBuffer writebuffer;

	private Metrics metrics;
	private Address local;
	private Configuration config;
	private String iface;
	private Selector sel;
	private DatagramChannel channel;

	private ConcurrentHashMap<Address, InetSocketAddress> addressBook = new ConcurrentHashMap<>();

	protected Logger log = Logger.getLogger(UDPNIO.class);

	private InetSocketAddress addr;

	public UDPNIO(Address local, Configuration config, String iface) {
		super("UDP NIO");
		this.local = local;
		this.config = config;
		this.iface = iface;
		this.readbuffer = ByteBuffer.allocate(config.max_msg_size + 32);
		// this.writebuffer = ByteBuffer.allocate(config.max_msg_size + 8 + 8);
		try {
			this.sel = Selector.open();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void setMetrics(Metrics metrics) {
		this.metrics = metrics;
	}

	@Override
	public void send(Message msg) throws Exception {

		InetSocketAddress to = null;
		if (msg.getSock() != null)
			to = msg.getSock().getSockTo();
		else
			to = addressBook.get(msg.getTo());
		byte[] ba = msg.toByteArray();
		edu.jlime.util.ByteBuffer toSend = new edu.jlime.util.ByteBuffer(
				ba.length + 32);

		toSend.putUUID(local.getId());
		toSend.putUUID(msg.getTo().getId());
		toSend.putRawByteArray(ba);
		ByteBuffer buff = ByteBuffer.wrap(toSend.build());

		// if (!channel.isOpen()) {
		// // log.warn("Channel is closed");
		// return;
		// }

		try {
			// synchronized (channel) {
			int write = 0;

			while ((write += channel.send(buff, to)) != toSend.size()) {
			}
			// }
		} catch (Exception e) {
			log.info("Closed UDP NIO Channel.");
		}

	}

	@Override
	public void onStart() throws Exception {
		for (int i = 0; i < config.port_range; i++) {
			try {

				this.channel = DatagramChannel.open();

				// Flags
				this.channel.setOption(StandardSocketOptions.SO_RCVBUF,
						config.rcvBuffer);
				this.channel.setOption(StandardSocketOptions.SO_SNDBUF,
						config.sendBuffer);
				this.addr = new InetSocketAddress(InetAddress.getByName(iface),
						config.port + i);
				this.channel.bind(addr);
				break;
			} catch (Exception e) {
				// e.printStackTrace();
			}
		}

		addressBook.put(local, addr);

		this.channel.configureBlocking(false);

		int interestSet = SelectionKey.OP_READ;

		this.channel.register(sel, interestSet);

		Thread t = new Thread("Nio Selector") {
			@Override
			public void run() {
				try {
					while (!stopped) {

						int readyChannels = sel.select();

						if (readyChannels == 0)
							continue;

						Set<SelectionKey> selectedKeys = sel.selectedKeys();

						Iterator<SelectionKey> keyIterator = selectedKeys
								.iterator();

						while (keyIterator.hasNext()) {
							SelectionKey key = (SelectionKey) keyIterator
									.next();

							if (key.isReadable()) {
								read(key.channel());
							}

							keyIterator.remove();
						}
					}
				} catch (Exception e) {
					log.info("Closed UDP NIO Selector.");
				}
			}
		};
		t.start();
	}

	private void read(SelectableChannel channel) throws Exception {
		DatagramChannel sc = (DatagramChannel) channel;
		readbuffer.clear();

		while (sc.receive(readbuffer) == null) {
		}
		readbuffer.flip();
		final byte[] b = new byte[readbuffer.remaining()];
		readbuffer.get(b, 0, b.length);
		final edu.jlime.util.ByteBuffer buff = new edu.jlime.util.ByteBuffer(b);

		final Address from = new Address(buff.getUUID());
		final Address to = new Address(buff.getUUID());

		if (!to.equals(local)) {
			if (log.isDebugEnabled())
				log.debug("Not for me.");
			return;
		}
		exec.execute(new Runnable() {
			@Override
			public void run() {
				Message msg = Message.deEncapsulate(buff.getRawByteArray(),
						from, local);
				try {
					notifyRcvd(msg);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});

	}

	@Override
	public List<SocketAddress> getAddresses() {
		ArrayList<SocketAddress> al = new ArrayList<>();
		try {
			al.add(new SocketAddress((InetSocketAddress) channel
					.getLocalAddress(), AddressType.UDPNIO));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return al;
	}

	@Override
	public AddressType getType() {
		return AddressType.UDPNIO;
	}

	@Override
	public void updateAddress(Address id, List<SocketAddress> addresses) {
		for (SocketAddress socketAddress : addresses) {
			addressBook.putIfAbsent(id, socketAddress.getSockTo());
		}
	}

	@Override
	protected void onStop() throws Exception {
		sel.close();
		exec.shutdown();
		channel.close();
	}

	@Override
	public void nodeFailed(Address node) {
		// TODO Auto-generated method stub

	}

}
