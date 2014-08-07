package edu.jlime.rpc;

import edu.jlime.rpc.message.AddressType;
import edu.jlime.rpc.message.Message;
import edu.jlime.rpc.message.MessageListener;
import edu.jlime.rpc.message.MessageProcessor;
import edu.jlime.rpc.multi.MultiInterface;

public class MultiInterfaceManager {

	public MultiInterface create(AddressType type, Configuration config,
			NetworkProtocolFactory factory) {
		MultiInterface iface = MultiInterface.create(type, config, factory);
		iface.addAllMessageListener(new MessageListener() {

			@Override
			public void rcv(Message msg, MessageProcessor origin)
					throws Exception {

			}
		});
		return iface;
	}
}
