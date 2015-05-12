package edu.jlime.rpc;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import edu.jlime.core.transport.Address;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.rpc.message.AddressType;
import edu.jlime.rpc.message.Message;
import edu.jlime.rpc.message.MessageProcessor;
import edu.jlime.rpc.message.SocketAddress;
import edu.jlime.util.DataTypeUtils;
import edu.jlime.util.RingQueue;

public class TCPNIO extends MessageProcessor implements AddressListProvider {
	private static final int SIZEOFACCEPTMESSAGE = 32;

	public static final int HEADER = 32;

	ByteBuffer readbuffer;

	private static class Event {
		Address from;
		Address to;
		SocketChannel channel;
		byte[] data;
	}

	RingQueue events = new RingQueue();

	// private ExecutorService exec = Executors
	// .newCachedThreadPool(new ThreadFactory() {
	//
	// @Override
	// public Thread newThread(Runnable r) {
	// Thread t = Executors.defaultThreadFactory().newThread(r);
	// t.setName("TCP NIO Worker Thread");
	// return t;
	// }
	// });

	private Metrics metrics;
	private Address local;
	private NetworkConfiguration config;
	private String iface;
	private Selector sel;
	private ServerSocketChannel channel;

	private ConcurrentHashMap<Address, InetSocketAddress> addressBook = new ConcurrentHashMap<>();

	private ConcurrentHashMap<SocketChannel, Address> fromHash = new ConcurrentHashMap<>();

	private Map<Address, List<Channel>> channels = new ConcurrentHashMap<>();

	protected Logger log = Logger.getLogger(TCPNIO.class);

	private List<Channel> toRegister = new ArrayList<>();

	public TCPNIO(Address local, NetworkConfiguration config, String iface) {
		super("UDP NIO");
		this.local = local;
		this.config = config;
		this.iface = iface;
		this.readbuffer = ByteBuffer.allocate(config.tcpnio_max_msg_size);
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
		// RingQueue map = q.get(msg.getTo());
		// if (map == null) {
		// synchronized (map) {
		// map = q.get(msg.getTo());
		// if (map == null) {
		// map = new RingQueue();
		// q.put(msg.getTo(), map);
		// }
		// }
		// }
		// map.put(msg);
		SocketChannel sc = null;
		List<Channel> list = channels.get(msg.getTo());
		if (list == null) {
			synchronized (channels) {
				list = channels.get(msg.getTo());
				if (list == null) {
					sc = SocketChannel.open();

					fromHash.put(sc, msg.getTo());

					sc.setOption(StandardSocketOptions.SO_RCVBUF,
							config.tcp_config.tcp_rcv_buffer);
					sc.setOption(StandardSocketOptions.SO_SNDBUF,
							config.tcp_config.tcp_send_buffer);
					sc.setOption(StandardSocketOptions.TCP_NODELAY, true);

					InetSocketAddress sockTo = null;
					SocketAddress sock = msg.getSock();
					if (sock != null) {
						sockTo = sock.getSockTo();
					} else {
						sockTo = addressBook.get(msg.getTo());
					}

					sc.connect(sockTo);

					sc.configureBlocking(false);

					edu.jlime.util.ByteBuffer buff = new edu.jlime.util.ByteBuffer(
							SIZEOFACCEPTMESSAGE);
					buff.putUUID(local.getId());
					buff.putUUID(msg.getTo().getId());
					ByteBuffer wrap = ByteBuffer.wrap(buff.build());

					synchronized (sc) {
						int write = 0;
						while ((write += sc.write(wrap)) != buff.size()) {
						}
					}

					synchronized (toRegister) {
						toRegister.add(sc);
					}
					sel.wakeup();

					// sc.register(sel, SelectionKey.OP_READ);

					list = new ArrayList<Channel>();
					list.add(sc);
					channels.put(msg.getTo(), list);
				}
			}
		}
		synchronized (list) {
			sc = (SocketChannel) list.get((int) (Math.random() * list.size()));
		}

		int size = msg.getSize()
		// + 32
		+ 4;
		edu.jlime.util.ByteBuffer[] msgAsBytes = msg.toByteBuffers();
		// byte[] ba = msg.toByteArray();

		edu.jlime.util.ByteBuffer toSend = new edu.jlime.util.ByteBuffer(32 + 4);
		toSend.putInt(
		// 32 +
		msg.getSize());
		// toSend.putUUID(local.getId());
		// toSend.putUUID(msg.getTo().getId());
		// toSend.putRawByteArray(ba);

		// ByteBuffer buff = ByteBuffer.wrap(toSend.build());

		ByteBuffer[] buff = new ByteBuffer[1 + msgAsBytes.length];
		// buff[0] = ByteBuffer.wrap(toSend.build());´
		buff[0] = toSend.asByteBuffer();
		for (int i = 0; i < msgAsBytes.length; i++)
			// buff[i + 1] = ByteBuffer.wrap(msgAsBytes[i].build());
			buff[i + 1] = msgAsBytes[i].asByteBuffer();

		// if (!sc.isConnected())
		// return;

		try {
			synchronized (sc) {
				int write = 0;
				while ((write += sc.write(buff)) != size) {
				}
			}

		} catch (Exception e) {
			sc.close();
		}

	}

	@Override
	public void onStart() throws Exception {
		for (int i = 0; i < config.port_range; i++) {
			try {

				this.channel = ServerSocketChannel.open();

				// Flags
				// this.channel.setOption(StandardSocketOptions.SO_RCVBUF,
				// config.rcvBuffer);
				// this.channel.setOption(StandardSocketOptions.SO_SNDBUF,
				// config.sendBuffer);
				// this.channel.setOption(StandardSocketOptions.TCP_NODELAY,
				// true);

				this.channel.bind(new InetSocketAddress(InetAddress
						.getByName(iface), config.port + i));
				break;
			} catch (Exception e) {
				// e.printStackTrace();
			}
		}

		this.channel.configureBlocking(false);

		int interestSet = SelectionKey.OP_ACCEPT;

		this.channel.register(sel, interestSet);

		Thread t = new Thread("Nio Selector") {
			@Override
			public void run() {
				try {
					while (!stopped) {
						int readyChannels = sel.select(1000);

						synchronized (toRegister) {
							Iterator<Channel> it = toRegister.iterator();
							while (it.hasNext()) {
								SocketChannel channel = (SocketChannel) it
										.next();
								sel.wakeup();
								channel.register(sel, SelectionKey.OP_READ);
								it.remove();
							}
						}

						if (readyChannels == 0) {
							Thread.sleep(0, 100);
							continue;
						}

						Set<SelectionKey> selectedKeys = sel.selectedKeys();

						Iterator<SelectionKey> keyIterator = selectedKeys
								.iterator();

						while (keyIterator.hasNext()) {
							SelectionKey key = (SelectionKey) keyIterator
									.next();

							if (key.isAcceptable()) {
								SocketChannel sock = ((ServerSocketChannel) key
										.channel()).accept();
								sock.setOption(StandardSocketOptions.SO_RCVBUF,
										config.tcp_config.tcp_rcv_buffer);
								sock.setOption(StandardSocketOptions.SO_SNDBUF,
										config.tcp_config.tcp_send_buffer);
								sock.setOption(
										StandardSocketOptions.TCP_NODELAY, true);
								sock.configureBlocking(false);
								accept(sock);
								sel.wakeup();
								synchronized (toRegister) {
									toRegister.add(sock);
								}
							} else if (key.isConnectable()) {
								// a connection was established with a remote
								// server.

							} else if (key.isReadable()) {
								read((SocketChannel) key.channel());
							} else if (key.isWritable()) {

							}

							keyIterator.remove();
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

		};
		t.start();
	}

	protected void accept(SocketChannel sock) throws IOException {
		readbuffer.position(0);
		readbuffer.limit(SIZEOFACCEPTMESSAGE);
		while (readbuffer.hasRemaining()) {
			int read = sock.read(readbuffer);
			if (read < 0) {
				sock.close();
				return;
			}
		}
		readbuffer.rewind();
		byte[] addrBytes = new byte[SIZEOFACCEPTMESSAGE];
		readbuffer.get(addrBytes, 0, SIZEOFACCEPTMESSAGE);
		edu.jlime.util.ByteBuffer buff = new edu.jlime.util.ByteBuffer(
				addrBytes);
		Address from = new Address(buff.getUUID());
		Address to = new Address(buff.getUUID());

		if (!to.equals(this.local)) {
			log.error("Connection not for me.");
			return;
		}

		List<Channel> list = channels.get(from);
		if (list == null) {
			synchronized (channels) {
				list = channels.get(from);
				if (list == null) {
					list = new ArrayList<Channel>();
					channels.put(from, list);
				}
			}
		}
		list.add(sock);
		fromHash.put(sock, from);
	}

	private void read(SocketChannel channel) throws Exception {
		final Address from = fromHash.get(channel);
		readbuffer.clear();

		SocketChannel sc = (SocketChannel) channel;
		// assuming buffer is a ByteBuffer
		readbuffer.position(0);
		readbuffer.limit(4);
		while (readbuffer.hasRemaining()) {
			try {
				int read = sc.read(readbuffer);
				if (read == -1) {
					sc.close();
					return;
				}
			} catch (Exception e) {
				sc.close();
				return;
			}
		}
		readbuffer.rewind();
		byte[] intasarray = new byte[4];

		readbuffer.get(intasarray, 0, 4);

		// get the byte and cast it into the range 0-255
		int length = DataTypeUtils.byteArrayToInt(intasarray);
		// System.out.println(length);
		readbuffer.clear();
		readbuffer.position(0);
		readbuffer.limit(length);
		while (readbuffer.hasRemaining()) {
			try {
				int read = sc.read(readbuffer);
				if (read == -1) {
					sc.close();
					return;
				}
			} catch (Exception e) {
				sc.close();
				return;
			}
		}
		readbuffer.rewind();
		// the buffer is now ready for reading the data from

		final byte[] b = new byte[length];
		readbuffer.get(b, 0, b.length);

		// exec.execute(new Runnable() {
		//
		// @Override
		// public void run() {
		final edu.jlime.util.ByteBuffer buff = new edu.jlime.util.ByteBuffer(b);

		// final Address from = new Address(buff.getUUID());
		// final Address to = new Address(buff.getUUID());

		// if (!to.equals(local)) {
		// if (log.isDebugEnabled())
		// log.debug("Not for me.");
		// return;
		// }

		Message msg = Message
				.deEncapsulate(buff.getRawByteArray(), from, local);
		try {
			notifyRcvd(msg);
		} catch (Exception e) {
			e.printStackTrace();
		}
		// }
		// });

	}

	@Override
	public List<SocketAddress> getAddresses() {
		ArrayList<SocketAddress> al = new ArrayList<>();
		try {
			al.add(new SocketAddress((InetSocketAddress) channel
					.getLocalAddress(), AddressType.TCPNIO));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return al;
	}

	@Override
	public AddressType getType() {
		return AddressType.TCPNIO;
	}

	@Override
	public void updateAddress(Address id, List<SocketAddress> addresses) {
		for (SocketAddress socketAddress : addresses) {
			addressBook.put(id, socketAddress.getSockTo());
		}
	}

	@Override
	protected void onStop() throws Exception {
		sel.close();
		// exec.shutdown();
		channel.close();
		for (List<Channel> l : channels.values()) {
			for (Channel channel : l) {
				channel.close();
			}
		}
	}
}
