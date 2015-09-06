package edu.jlime.rpc.tcp;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

import edu.jlime.rpc.SocketFactory;

class TCPSocketFactory extends SocketFactory {

	private int rcv_buffer;

	public TCPSocketFactory(int tcp_rcv_buffer) {
		this.rcv_buffer = tcp_rcv_buffer;
	}

	@Override
	public jLimeSocket getSocket(String addr, int port) throws Exception {
		ServerSocket socket = new ServerSocket();
		// socket.setReuseAddress(true);
		socket.setReceiveBufferSize(rcv_buffer);
		socket.bind(new InetSocketAddress(InetAddress.getByName(addr), port));
		return new jLimeSocket(socket, port, addr);
	}
}