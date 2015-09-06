package edu.jlime.rpc;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Test;

import edu.jlime.metrics.metric.Metrics;
import edu.jlime.rpc.message.Message;
import edu.jlime.rpc.message.MessageListener;
import edu.jlime.rpc.message.MessageProcessor;
import edu.jlime.rpc.message.MessageType;
import edu.jlime.util.DataTypeUtils;

public class MessageProcessorPerformanceTest {

	long init;

	long end;

	private int ITERMAX = 3000000;

	Set<Message> set = Collections.synchronizedSet(new HashSet<Message>());

	private MessageProcessor mp;

	public MessageProcessorPerformanceTest() throws Exception {
		mp = new MessageProcessor("Algo") {
			@Override
			public void send(Message msg) throws Exception {
				notifyRcvd(msg);
			}

			@Override
			public void setMetrics(Metrics metrics) {

			}
		};

		mp.addAllMessageListener(new MessageListener() {
			int msgCount = 0;

			@Override
			public void rcv(Message msg, MessageProcessor origin)
					throws Exception {
				// System.out.println(msg.getDataReader().getInt());
				set.add(msg);
				msgCount++;
				if (msgCount == ITERMAX)
					received();
			}
		});
		Thread t = new Thread() {
			public void run() {
				int iter = ITERMAX / 2;
				while (iter < ITERMAX) {
					try {
						mp.send(Message.newOutDataMessage(
								DataTypeUtils.intToByteArray(iter),
								MessageType.ACK, null));
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					iter++;
				}
			};
		};

		init = System.nanoTime();
		t.start();
		int iter = 0;
		while (iter < ITERMAX / 2) {
			mp.send(Message.newOutDataMessage(
					DataTypeUtils.intToByteArray(iter), MessageType.ACK, null));
			iter++;
		}
	}

	protected void received() {
		try {
			end = System.nanoTime();
			long dur = System.nanoTime() - init;
			System.out.println(dur / 1000 / 1000 + " ms");
			System.out.println(dur / ITERMAX + " ns");
			TreeSet<Integer> treeSet = new TreeSet<>();
			for (Message m : set) {
				treeSet.add(m.getDataBuffer().getInt());
			}
			System.out.println(treeSet.size());
			treeSet.clear();
			set.clear();
			// for (Integer integer : treeSet) {
			// System.out.println(integer);
			// }

		} catch (Exception e) {
			e.printStackTrace();
		}
		// System.exit(0);
	}

	public static void main(String[] args) throws Exception {
		new MessageProcessorPerformanceTest();
		new ArrayBlockingQueueTest().algo();
	}

	@Test
	public void runMPTest() throws Exception {
		new MessageProcessorPerformanceTest();
	}
}
