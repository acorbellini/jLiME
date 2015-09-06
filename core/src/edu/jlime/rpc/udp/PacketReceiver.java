package edu.jlime.rpc.udp;

import java.net.DatagramPacket;

public interface PacketReceiver {

	public void datagramReceived(DatagramPacket packet) throws Exception;
}
