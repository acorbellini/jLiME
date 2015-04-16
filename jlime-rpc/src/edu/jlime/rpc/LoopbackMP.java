package edu.jlime.rpc;

import edu.jlime.metrics.metric.Metrics;
import edu.jlime.rpc.message.Message;
import edu.jlime.rpc.message.SimpleMessageProcessor;
import edu.jlime.util.ByteBuffer;

public class LoopbackMP extends SimpleMessageProcessor {

	public LoopbackMP() {
		super(null, "Local Message Processor");
	}

	@Override
	public void setMetrics(Metrics metrics) {
	}

	@Override
	public void send(Message msg) throws Exception {
		byte[] ba = msg.toByteArray();
		notifyRcvd(Message.deEncapsulate(new ByteBuffer(ba), msg.getFrom(),
				msg.getTo()));
	}
}
