package edu.jlime.rpc.udp;

import java.net.DatagramPacket;
import java.net.DatagramSocket;

import edu.jlime.util.RingQueue;

public class DatagramReceiver {

	private DatagramSocket sock;

	private RingQueue packets = new RingQueue();

	private int buff_size;

	private volatile boolean stopped = false;

	private PacketReceiver rcvr;

	public DatagramReceiver(DatagramSocket sock, int buff_size,
			PacketReceiver receiver) {
		this.rcvr = receiver;
		this.sock = sock;
		this.buff_size = buff_size;
		Thread read = new Thread("DEF UDP Socket Reader") {
			@Override
			public void run() {
				while (!stopped)
					read();
			}
		};
		// read.setDaemon(true);
		read.start();

		Thread consume = new Thread("DEF UDP Datagram Receiver") {
			@Override
			public void run() {
				while (!stopped)
					try {
						Object[] array = packets.get();
						for (Object object : array) {
							DatagramPacket pkt = (DatagramPacket) object;
							if (stopped)
								return;
							rcvr.datagramReceived(pkt);
						}

					} catch (Exception e) {
						e.printStackTrace();
					}
			}
		};
		// consume.setDaemon(true);
		consume.start();
	}

	public void read() {
		byte[] b = new byte[buff_size];
		DatagramPacket d = new DatagramPacket(b, buff_size);
		try {
			sock.receive(d);
			packets.add(d);
		} catch (Exception e) {
		}

	}

	public void setStopped() {
		this.stopped = true;
		// try {
		packets.add(new DatagramPacket(new byte[] {}, 0));
		// } catch (InterruptedException e) {
		// e.printStackTrace();
		// }
	}
}