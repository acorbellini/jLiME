package edu.jlime.rpc.udp;

import java.net.DatagramPacket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

public class PacketReader extends Thread {

	ExecutorService exec = Executors.newFixedThreadPool(20);

	private LinkedBlockingDeque<DatagramPacket> packets;

	private PacketReceiver listener;

	public PacketReader(LinkedBlockingDeque<DatagramPacket> packets,
			PacketReceiver listener) {
		super("Datagram Packet Reader");
		this.packets = packets;
		this.listener = listener;
	}

	@Override
	public void run() {
		while (true) {
			try {
				final DatagramPacket p = packets.takeLast();
				exec.execute(new Runnable() {
					@Override
					public void run() {
						try {
							listener.datagramReceived(p);
						} catch (Exception e) {
							e.printStackTrace();
						}

					}
				});
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	}

}