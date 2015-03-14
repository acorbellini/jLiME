package edu.jlime.rpc;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import edu.jlime.core.transport.Address;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.rpc.ProtocolHost.ProtocolHandle;
import edu.jlime.rpc.message.AddressType;
import edu.jlime.rpc.message.Message;
import edu.jlime.rpc.message.MessageProcessor;
import edu.jlime.util.ByteBuffer;

public class JNET extends MessageProcessor implements AddressListProvider,
		HandleListener<byte[]> {
	private ConcurrentHashMap<Address, InetSocketAddress> addressBook = new ConcurrentHashMap<>();

	private ConcurrentHashMap<Address, ProtocolHost.ProtocolHandle<byte[]>> handles = new ConcurrentHashMap<>();

	ProtocolHost<byte[]> localProto;

	private Address local;
	private Configuration config;
	private String iface;

	private InetSocketAddress addr;

	public JNET(Address local, Configuration config, String iface) {
		super("JNET Message Processor");
		this.local = local;
		this.config = config;
		this.iface = iface;
	}

	@Override
	public void onStart() throws Exception {
		for (int i = 0; i < config.port_range; i++) {
			try {
				this.addr = new InetSocketAddress(InetAddress.getByName(iface),
						config.port + i);
				this.localProto = new ProtocolHost<byte[]>(local.toString(),
						byte[].class, this.addr);
				break;
			} catch (Exception e) {
				// e.printStackTrace();
			}
		}

		handles.put(local, localProto.register(local, addr));

		localProto.addHandleListener(this);

		Thread t = new Thread() {
			public void run() {
				while (!stopped) {
					for (ProtocolHandle<byte[]> h : handles.values()) {
						try {
							byte[] data = null;
							synchronized (h) {
								data = h.receive();
							}
							if (data != null) {
								ByteBuffer buff = new ByteBuffer(data);
								Address from = new Address(buff.getUUID());
								Address to = new Address(buff.getUUID());
								Message msg = Message.deEncapsulate(
										buff.getRawByteArray(), from, local);
								notifyRcvd(msg);
							}
						} catch (ClassNotFoundException | IOException e) {
							e.printStackTrace();
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
					try {
						Thread.sleep(5);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			};
		};
		t.start();
	}

	@Override
	public void setMetrics(Metrics metrics) {
		// TODO Auto-generated method stub

	}

	@Override
	public List<edu.jlime.rpc.message.SocketAddress> getAddresses() {
		ArrayList<edu.jlime.rpc.message.SocketAddress> al = new ArrayList<>();
		al.add(new edu.jlime.rpc.message.SocketAddress(addr, AddressType.JNET));
		return al;
	}

	@Override
	public AddressType getType() {
		return AddressType.JNET;
	}

	@Override
	public void send(Message msg) throws Exception {
		ProtocolHandle<byte[]> handle = handles.get(msg.getTo());
		if (handle == null) {
			synchronized (handles) {
				handle = handles.get(msg.getTo());
				if (handle == null) {
					InetSocketAddress to = null;
					if (msg.getSock() != null)
						to = msg.getSock().getSockTo();
					else
						to = addressBook.get(msg.getTo());
					handle = localProto.register(msg.getTo(), to);
					handles.put(msg.getTo(), handle);
				}
			}
		}
		synchronized (localProto) {
			edu.jlime.util.ByteBuffer buff = new edu.jlime.util.ByteBuffer();
			buff.putUUID(local.getId());
			buff.putUUID(msg.getTo().getId());
			buff.putRawByteArray(msg.toByteArray());
			handle.send(buff.build());
		}
	}

	@Override
	public void updateAddress(Address id,
			List<edu.jlime.rpc.message.SocketAddress> addresses) {
		for (edu.jlime.rpc.message.SocketAddress socketAddress : addresses) {
			addressBook.putIfAbsent(id, socketAddress.getSockTo());
		}
	}

	@Override
	public void added(ProtocolHandle<byte[]> handle) {
		handles.put(handle.getProtocolId().getTopic(), handle);
	}

}
