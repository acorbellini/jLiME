package edu.jlime.core.transport;

import java.net.InetSocketAddress;

public interface DataReceiver {

	public void dataReceived(byte[] array, InetSocketAddress addr);

}
