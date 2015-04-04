package edu.jlime.rpc.rabbit;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

import edu.jlime.core.transport.Address;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.rpc.AddressListProvider;
import edu.jlime.rpc.Configuration;
import edu.jlime.rpc.message.AddressType;
import edu.jlime.rpc.message.Message;
import edu.jlime.rpc.message.MessageProcessor;
import edu.jlime.rpc.message.SocketAddress;
import edu.jlime.util.ByteBuffer;

public class RabbitProcessor extends MessageProcessor implements
		AddressListProvider {

	Logger log = Logger.getLogger(RabbitProcessor.class);

	private ConcurrentHashMap<Address, Channel> addressBook = new ConcurrentHashMap<>();

	private Configuration config;
	private String iface;
	private Address local;
	private Connection connection;
	private Channel channel;
	private String replyQueueName;
	private QueueingConsumer consumer;

	public RabbitProcessor(Configuration config, String iface, Address local) {
		super("Rabbit Processor");
		this.config = config;
		this.iface = iface;
		this.local = local;
	}

	@Override
	public void setMetrics(Metrics metrics) {
	}

	@Override
	public void send(Message msg) throws Exception {
		Channel to = addressBook.get(msg.getTo());
		if (to == null) {
			synchronized (addressBook) {
				to = addressBook.get(msg.getTo());
				if (to == null) {
					InetSocketAddress sockTo = msg.getSock().getSockTo();
					to = createRabbit(msg.getTo(), sockTo);

				}
			}
		}

		byte[] array = msg.toByteArray();

		ByteBuffer writer = new ByteBuffer(array.length + 32);
		writer.putUUID(local.getId());
		writer.putUUID(msg.getTo().getId());
		writer.putRawByteArray(array);
		byte[] built = writer.build();

		to.basicPublish("", msg.getTo().toString(), null, built);
	}

	private Channel createRabbit(Address id, InetSocketAddress sockTo)
			throws IOException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(sockTo.getHostName());
		// This should be part of the config.
		factory.setUsername("admin");
		factory.setPassword("admin");
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();
		// channel.queueDeclare(id.toString(), false, false, false, null);
		addressBook.put(id, channel);
		return channel;
	}

	@Override
	public List<SocketAddress> getAddresses() {
		ArrayList<SocketAddress> ret = new ArrayList<>();
		try {
			ret.add(new SocketAddress(new InetSocketAddress(InetAddress
					.getLocalHost(), 0), getType()));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return ret;
	}

	@Override
	public AddressType getType() {
		return AddressType.RABBIT;
	}

	@Override
	public void updateAddress(Address id, List<SocketAddress> addresses) {
		for (SocketAddress socketAddress : addresses) {
			synchronized (addressBook) {
				if (!addressBook.containsKey(id)) {
					try {
						createRabbit(id, socketAddress.getSockTo());
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}

	@Override
	public void onStart() throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(InetAddress.getLocalHost().getHostName());
		factory.setUsername("admin");
		factory.setPassword("admin");
		connection = factory.newConnection();
		channel = connection.createChannel();

		replyQueueName = channel.queueDeclare(local.toString(), false, false,
				true, null).getQueue();
		consumer = new QueueingConsumer(channel);
		channel.basicConsume(replyQueueName, true, consumer);

		Thread t = new Thread() {
			public void run() {
				while (!stopped) {
					try {
						QueueingConsumer.Delivery delivery = consumer
								.nextDelivery();
						final edu.jlime.util.ByteBuffer buff = new edu.jlime.util.ByteBuffer(
								delivery.getBody());

						final Address from = new Address(buff.getUUID());
						final Address to = new Address(buff.getUUID());

						if (!to.equals(local)) {
							if (log.isDebugEnabled())
								log.debug("Not for me.");
							return;
						}
						Message msg = Message.deEncapsulate(buff, from, local);
						try {
							notifyRcvd(msg);
						} catch (Exception e) {
							e.printStackTrace();
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			};
		};
		t.start();
	}

	@Override
	protected void onStop() throws Exception {
		channel.queueDelete(local.toString());
		channel.close();
		connection.close();
	}
}
