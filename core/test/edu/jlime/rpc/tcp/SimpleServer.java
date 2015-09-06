package edu.jlime.rpc.tcp;

import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class SimpleServer {
	public static void main(String[] args) throws Exception {
		ServerSocket server = new ServerSocket(6666);
		Socket socket = server.accept();
		OutputStream output = socket.getOutputStream();

		byte[] bytes = new byte[32 * 1024]; // 32K
		while (true) {
			output.write(bytes);
		}
	}
}
