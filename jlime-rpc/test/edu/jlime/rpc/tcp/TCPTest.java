package edu.jlime.rpc.tcp;

import edu.jlime.core.transport.Address;
import edu.jlime.rpc.Configuration;
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
		Address id = new Address();
		Address id2 = new Address();
		final TCP tcp = new TCP(id, addr, 8000, 1,
				new Configuration(null).tcp_config);

		TCP tcp2 = new TCP(id2, addr, 8001, 1,
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

		Message newFullDataMessage = Message.newFullDataMessage(
				"Hola Nenenene".getBytes(), MessageType.DATA, id, id2);
		newFullDataMessage.setInetSocketAddress(tcp2.getAddress());
		tcp2.send(newFullDataMessage);

	}
}
