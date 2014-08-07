package edu.jlime.rpc.tcp;

import java.net.InetSocketAddress;

public interface DataReceiver {

	public void dataReceived(byte[] array, InetSocketAddress addr);

}
