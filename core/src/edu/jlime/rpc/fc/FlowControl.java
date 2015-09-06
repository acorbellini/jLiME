package edu.jlime.rpc.fc;

import java.util.HashMap;

import org.apache.log4j.Logger;

import edu.jlime.core.transport.Address;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.rpc.message.Message;
import edu.jlime.rpc.message.MessageListener;
import edu.jlime.rpc.message.MessageProcessor;
import edu.jlime.rpc.message.MessageType;
import edu.jlime.rpc.message.SimpleMessageProcessor;
import edu.jlime.util.ByteBuffer;

public class FlowControl extends SimpleMessageProcessor {

	static Logger log = Logger.getLogger(FlowControl.class);

	HashMap<Address, FlowControlPerNode> fcPerNode = new HashMap<>();

	private FCConfiguration config;

	public FlowControl(MessageProcessor next, FCConfiguration config) {
		super(next, "Flow Control");
		this.config = config;
	}

	@Override
	public void onStart() throws Exception {
		getNext().addMessageListener(MessageType.FC, new MessageListener() {
			@Override
			public void rcv(Message msg, MessageProcessor origin)
					throws Exception {
				FlowControlPerNode fc = getFC(msg.getFrom());
				ByteBuffer reader = msg.getHeaderBuffer();
				int max_send = reader.getInt();
				fc.update(msg.getDataSize(), max_send);
				notifyRcvd(Message.deEncapsulate(msg.getDataAsBytes(),
						msg.getFrom(), msg.getTo()));

			}
		});

		getNext().addMessageListener(MessageType.FC_ACK, new MessageListener() {
			@Override
			public void rcv(Message msg, MessageProcessor origin)
					throws Exception {
				FlowControlPerNode fc = getFC(msg.getFrom());
				fc.ackRcvd();
			}
		});
	}

	@Override
	public void send(Message msg) throws Exception {
		Address to = msg.getTo();
		FlowControlPerNode fc = getFC(to);
		fc.send(msg);
	}

	private FlowControlPerNode getFC(Address to) throws Exception {
		FlowControlPerNode fc;
		synchronized (fcPerNode) {
			fc = fcPerNode.get(to);
			if (fc == null) {
				fc = new FlowControlPerNode(to, getNext(), config);
				fcPerNode.put(to, fc);
				fc.start();
			}
		}
		return fc;
	}

	@Override
	public void cleanupOnFailedPeer(Address addr) {
		try {
			FlowControlPerNode fc = getFC(addr);
			if (fc != null)
				fc.onStop();
			fcPerNode.remove(addr);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void setMetrics(Metrics metrics) {

	}
}
