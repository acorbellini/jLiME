package edu.jlime.rpc.udp;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.apache.log4j.Logger;

import edu.jlime.util.RingQueue;

public class DatagramReceiver {

	private DatagramSocket sock;

	private int buff_size;

	private volatile boolean stopped = false;

	private PacketReceiver rcvr;

	RingQueue packets = new RingQueue();

	private Logger log = Logger.getLogger(DatagramReceiver.class);

	ExecutorService exec = Executors.newCachedThreadPool(new ThreadFactory() {

		@Override
		public Thread newThread(Runnable r) {
			Thread t = Executors.defaultThreadFactory().newThread(r);
			t.setName("UDP Datagram Thread");
			return t;
		}
	});

	public DatagramReceiver(DatagramSocket sock, int buff_size,
			PacketReceiver receiver) {
		this.rcvr = receiver;
		this.sock = sock;
		this.buff_size = buff_size;
		Thread read = new Thread("UDP Socket Reader") {
			@Override
			public void run() {

				try {
					while (!stopped)
						read();
				} catch (Exception e) {
					if (log.isDebugEnabled())
						log.debug("Error reading from datagram socket "
								+ e.getMessage());
				}
			}
		};
		// read.setDaemon(true);
		read.start();

		Thread consume = new Thread("UDP Datagram Receiver") {
			@Override
			public void run() {
				while (!stopped)
					try {
						Object[] array = packets.take();
						for (Object object : array) {
							final DatagramPacket pkt = (DatagramPacket) object;
							if (stopped)
								return;
							exec.execute(new Runnable() {

								@Override
								public void run() {

									if (stopped)
										return;
									try {
										rcvr.datagramReceived(pkt);
									} catch (Exception e) {
										e.printStackTrace();
									}
								}
							});
						}

					} catch (Exception e) {
						e.printStackTrace();
					}
			}
		};
		consume.setDaemon(true);
		consume.start();
	}

	public void read() throws Exception {
		byte[] b = new byte[buff_size];
		final DatagramPacket d = new DatagramPacket(b, buff_size);
		sock.receive(d);
		packets.put(d);
		// exec.execute(new Runnable() {
		// @Override
		// public void run() {
		// try {
		// rcvr.datagramReceived(d);
		// } catch (Exception e) {
		// e.printStackTrace();
		// }
		// }
		// });

	}

	public void setStopped() {
		exec.shutdown();
		this.stopped = true;
		packets.put(new DatagramPacket(new byte[] {}, 0));
	}
}