package edu.jlime.rpc;

import java.util.UUID;

import edu.jlime.core.transport.Address;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.rpc.message.Message;
import edu.jlime.rpc.message.MessageProcessor;
import edu.jlime.rpc.message.MessageType;
import edu.jlime.rpc.message.SimpleMessageProcessor;

public class MPChainPerfTest {

	private static final int MSGS = 90000000;

	int count = 1;

	long init = 0;

	public MPChainPerfTest() throws Exception {

		MessageProcessor last = new SimpleMessageProcessor(null, "Final") {
			@Override
			public void send(Message msg) throws Exception {
				if (count < MSGS)
					count++;
				else {
					long time = System.nanoTime();
					System.out.println("Aprox time to send " + count + " msgs "
							+ (time - init) / MSGS + " ns");
				}
			}

			@Override
			public void setMetrics(Metrics metrics) {

			}
		};

		SimpleMessageProcessor first = null;
		for (int i = 0; i < 10; i++) {
			first = new SimpleMessageProcessor(last, "Intermedio") {
				@Override
				public void send(Message msg) throws Exception {
					// System.out.println("Sending to Next");
					sendNext(msg);
				}

				@Override
				public void setMetrics(Metrics metrics) {

				}
			};
			last = first;
		}
		init = System.nanoTime();
		for (int i = 1; i <= MSGS; i++) {
			first.send(Message.newEmptyOutDataMessage(MessageType.ACK,
					new Address(UUID.randomUUID())));
		}

	}

	public static void main(String[] args) throws Exception {
		new MPChainPerfTest();

	}
}
