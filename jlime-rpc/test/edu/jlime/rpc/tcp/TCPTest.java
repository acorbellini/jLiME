package edu.jlime.rpc.tcp;

import edu.jlime.rpc.Configuration;
import edu.jlime.rpc.message.JLiMEAddress;
import edu.jlime.rpc.message.Message;
import edu.jlime.rpc.message.MessageListener;
import edu.jlime.rpc.message.MessageProcessor;
import edu.jlime.rpc.message.MessageType;
import edu.jlime.util.NetworkUtils;

public class TCPTest {
	public TCPTest() throws Exception {

	}

	public static void main(String[] args) throws Exception {
		new TCPTest().run();
	}

	private void run() throws Exception {
		String addr = NetworkUtils.getFirstHostAddress();
		final TCP tcp = new TCP(new JLiMEAddress(), addr, 8000, 1,
				new Configuration(null).tcp_config);

		TCP tcp2 = new TCP(new JLiMEAddress(), addr, 8001, 1,
				new Configuration(null).tcp_config);
		tcp.addAllMessageListener(new MessageListener() {

			@Override
			public void rcv(Message msg, MessageProcessor origin)
					throws Exception {
				String s = new String(msg.getDataAsBytes());
				System.out.println(s);

			}
		});

		tcp.start();
		tcp2.start();

		tcp2.send(Message.newFullDataMessage("Hola Nenenene".getBytes(),
				MessageType.DATA, tcp2.getAddress(), tcp.getAddress()));

	}
}
