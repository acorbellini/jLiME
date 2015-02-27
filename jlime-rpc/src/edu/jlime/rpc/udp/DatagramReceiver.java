package edu.jlime.rpc.udp;

import java.net.DatagramPacket;
import java.net.DatagramSocket;

import org.apache.log4j.Logger;

import edu.jlime.util.RingQueue;

public class DatagramReceiver {

	private DatagramSocket sock;

	// private RingQueue packets = new RingQueue();

	private int buff_size;

	private volatile boolean stopped = false;

	private PacketReceiver rcvr;

	private Logger log = Logger.getLogger(DatagramReceiver.class);

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

		// Thread consume = new Thread("UDP Datagram Receiver") {
		// @Override
		// public void run() {
		// while (!stopped)
		// try {
		// Object[] array = packets.take();
		// for (Object object : array) {
		// DatagramPacket pkt = (DatagramPacket) object;
		// if (stopped)
		// return;
		// rcvr.datagramReceived(pkt);
		// }
		//
		// } catch (Exception e) {
		// e.printStackTrace();
		// }
		// }
		// };
		// consume.setDaemon(true);
		// consume.start();
	}

	public void read() throws Exception {
		byte[] b = new byte[buff_size];
		DatagramPacket d = new DatagramPacket(b, buff_size);
		sock.receive(d);
		// packets.put(d);
		rcvr.datagramReceived(d);
	}

	public void setStopped() {
		this.stopped = true;
		// try {
		// packets.put(new DatagramPacket(new byte[] {}, 0));
		// } catch (InterruptedException e) {
		// e.printStackTrace();
		// }
	}
}