package edu.jlime.rpc.zeromq;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import edu.jlime.core.transport.Address;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.rpc.AddressListProvider;
import edu.jlime.rpc.NetworkConfiguration;
import edu.jlime.rpc.message.AddressType;
import edu.jlime.rpc.message.Message;
import edu.jlime.rpc.message.MessageProcessor;
import edu.jlime.rpc.message.SocketAddress;
import edu.jlime.util.ByteBuffer;

public class ZeroMQProcessor extends MessageProcessor implements
		AddressListProvider {

	private Context context;
	private Socket responder;
	private NetworkConfiguration config;
	private String iface;
	private Address local;
	Logger log = Logger.getLogger(ZeroMQProcessor.class);
	ExecutorService exec = Executors.newCachedThreadPool();
	private ConcurrentHashMap<Address, Socket> addressBook = new ConcurrentHashMap<>();
	private SocketAddress socket;

	public ZeroMQProcessor(NetworkConfiguration config, String iface, Address local) {
		super("ZeroMQ");
		this.config = config;
		this.iface = iface;
		this.local = local;
		// TODO Auto-generated constructor stub
	}

	@Override
	public void setMetrics(Metrics metrics) {
		// TODO Auto-generated method stub

	}

	@Override
	public void send(Message msg) throws Exception {
		Socket to = addressBook.get(msg.getTo());
		if (to == null) {
			synchronized (addressBook) {
				to = addressBook.get(msg.getTo());
				if (to == null) {
					InetSocketAddress sockTo = msg.getSock().getSockTo();
					to = getZeroMQSocket(sockTo);
					addressBook.put(msg.getTo(), to);

				}
			}
		}

		byte[] array = msg.toByteArray();

		ByteBuffer writer = new ByteBuffer(array.length + 32);
		writer.putUUID(local.getId());
		writer.putUUID(msg.getTo().getId());
		writer.putRawByteArray(array);
		byte[] built = writer.build();

		try {
			to.send(built, 0);
			to.recv();
		} catch (Exception e) {
			// log.info("Closed UDP NIO Channel.");
		}
	}

	private Socket getZeroMQSocket(InetSocketAddress sockTo) {
		Socket to;
		to = context.socket(ZMQ.REQ);
		to.connect("tcp://" + sockTo.getHostName() + ":" + sockTo.getPort());
		return to;
	}

	@Override
	public void onStart() throws Exception {
		System.load("C:/Program Files/ZeroMQ 4.0.4/bin/libzmq-v120-mt-gd-4_0_4.dll");
		System.load("C:/Users/Alejandro/Dropbox/Desarrollo/jLiME/jlime-rpc/lib/zmq/jzmq.dll");

		this.context = ZMQ.context(1);

		// Socket to talk to server
		this.responder = context.socket(ZMQ.REP);
		for (int i = 0; i < config.port_range; i++) {
			try {
				responder.bind("tcp://" + iface + ":" + (config.port + i));
				this.socket = new SocketAddress(new InetSocketAddress(iface,
						config.port + i), getType());
				break;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		Thread t = new Thread() {
			@Override
			public void run() {
				while (!stopped) {
					// Wait for next request from client
					byte[] b = responder.recv();
					final edu.jlime.util.ByteBuffer buff = new edu.jlime.util.ByteBuffer(
							b);

					final Address from = new Address(buff.getUUID());
					final Address to = new Address(buff.getUUID());

					if (!to.equals(local)) {
						if (log.isDebugEnabled())
							log.debug("Not for me.");
						return;
					}
					responder.send("");

					exec.execute(new Runnable() {
						@Override
						public void run() {
							Message msg = Message.deEncapsulate(buff, from,
									local);
							try {
								notifyRcvd(msg);
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
					});
				}
			}
		};
		t.start();

	}

	@Override
	protected void onStop() throws Exception {
		responder.close();
		context.term();
	}

	@Override
	public List<SocketAddress> getAddresses() {
		ArrayList<SocketAddress> ret = new ArrayList<SocketAddress>();
		ret.add(socket);
		return ret;
	}

	@Override
	public AddressType getType() {
		return AddressType.ZMQ;
	}

	@Override
	public void updateAddress(Address id, List<SocketAddress> addresses) {
		for (SocketAddress socketAddress : addresses) {
			if (!addressBook.containsKey(id)) {
				addressBook.put(id, getZeroMQSocket(socketAddress.getSockTo()));
			}
		}
	}

}
