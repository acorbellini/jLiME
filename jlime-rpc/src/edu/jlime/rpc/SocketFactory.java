package edu.jlime.rpc;

import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;

public abstract class SocketFactory {

	public static class jLimeSocket {

		private Object javaSocket;

		private int port;

		private String addr;

		public jLimeSocket(Object javaSocket, int port, String addr) {
			super();
			this.javaSocket = javaSocket;
			this.port = port;
			this.addr = addr;
		}

		public String getAddr() {
			return addr;
		}

		public int getPort() {
			return port;
		}

		public Object getJavaSocket() {
			return javaSocket;
		}

	}

	public abstract jLimeSocket getSocket(String addr, int port)
			throws Exception;

	public static SocketFactory getMcastFactory(final String ifaddr,
			final int sendBuffer, final int receiveBuffer) {
		return new SocketFactory() {

			@Override
			public jLimeSocket getSocket(String addr, int port)
					throws Exception {
				MulticastSocket sock = new MulticastSocket(port);
				sock.setInterface(InetAddress.getByName(ifaddr));
				sock.setSendBufferSize(sendBuffer);
				sock.setReceiveBufferSize(receiveBuffer);
				sock.joinGroup(InetAddress.getByName(addr));
				return new jLimeSocket(sock, port, addr);
			}
		};

	}

	public static SocketFactory getUnicastFactory(final int sendBuffer,
			final int receiveBuffer) {
		return new SocketFactory() {

			@Override
			public jLimeSocket getSocket(String addr, int port)
					throws Exception {
				InetSocketAddress sockAddr = new InetSocketAddress(addr, port);
				DatagramSocket sock = new DatagramSocket(sockAddr);
				sock.setSendBufferSize(sendBuffer);
				sock.setReceiveBufferSize(receiveBuffer);
				return new jLimeSocket(sock, port, addr);
			}
		};

	}
}
