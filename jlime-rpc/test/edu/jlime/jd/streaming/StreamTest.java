package edu.jlime.jd.streaming;

import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Scanner;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import edu.jlime.client.Client;
import edu.jlime.client.JobContext;
import edu.jlime.core.cluster.StreamResult;
import edu.jlime.core.stream.RemoteInputStream;
import edu.jlime.core.stream.RemoteOutputStream;
import edu.jlime.jd.JobNode;
import edu.jlime.jd.job.StreamJob;
import edu.jlime.util.ByteBuffer;
import edu.jlime.util.RingQueue;

public class StreamTest {

	private static final int INT_ARRAY_SIZE = 1;
	private static final int BUFFER_SIZE = 1 * 1024 * 1024;
	private static final int READ_BUFFER = 512 * 1024;

	public static class StreamTestJob extends StreamJob {

		@Override
		public void run(RemoteInputStream inputStream,
				RemoteOutputStream outputStream, JobContext ctx)
				throws Exception {
			// Scanner scanner = new Scanner(inputStream);
			// try {
			// while (scanner.hasNext())
			// System.out.println(scanner.nextInt());
			// } catch (Exception e) {
			// e.printStackTrace();
			// }
			long init = System.nanoTime();
			long count = 0;
			// BufferedInputStream reader = new
			// BufferedInputStream(inputStream);
			InputStream reader = inputStream;
			// ((TCPInputStream) inputStream).getIs();
			try {
				byte[] four = new byte[READ_BUFFER];
				int read = 0;
				while ((read = reader.read(four)) != -1) {
					// for (int i = 0; i < read / 4; i++) {
					// IntUtils.byteArrayToInt(four, i * 4);
					// count++;
					// }
					count += read;

				}
			} catch (EOFException eof) {
				System.out.println("Finished reading");
			} catch (Exception e) {
				e.printStackTrace();
			}

			long megas = (count / (1024 * 1024));
			float time = (System.nanoTime() - init) / (float) (1000000000);
			System.out.println(megas / time + " mb/s");

			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
					outputStream));
			writer.write("Hello hello hello, is there anybody out there?");
			writer.close();
		}

	}

	public static void main(String[] args) throws Exception {
		Client cli = Client.build(1);

		long init = System.nanoTime();

		JobNode peer = cli.getCluster().getAnyExecutor();

		StreamTestJob stream = new StreamTestJob();

		StreamResult res = peer.stream(stream);

		// BufferedOutputStream buf = new BufferedOutputStream(res.getOs());
		// BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
		// res.getOs()));
		// writer.write("1 2 3 4 5 6");
		// writer.write(" 7 8 9");
		// writer.close();

		// BufferedOutputStream os = new BufferedOutputStream(res.getOs(),
		// 2 * 1024 * 1024);

		final RingQueue q = new RingQueue(10 * 1024);
		// final BufferedOutputStream os = new BufferedOutputStream(res.getOs(),
		// 32 * 1024);

		final OutputStream os = res.getOs();
		// ((TCPOutputStream) res.getOs()).getOs();

		final AtomicBoolean finished = new AtomicBoolean(false);
		final Semaphore sem = new Semaphore(0);
		Thread thread = new Thread() {
			public void run() {
				while (!finished.get() || !q.isEmpty()) {
					for (Object o : q.take()) {
						try {
							os.write((byte[]) o);
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
					// try {
					// os.flush();
					// } catch (IOException e) {
					// e.printStackTrace();
					// }
				}
				try {
					os.flush();
				} catch (IOException e) {
					e.printStackTrace();
				}
				sem.release();
			};
		};
		thread.start();

		byte[] ba = new byte[BUFFER_SIZE];
		int[] array = new int[INT_ARRAY_SIZE];
		for (int j = 0; j < array.length; j++) {
			array[j] = (int) (Math.random() * 1000);
		}
		ByteBuffer buffer = new ByteBuffer(4 * 8 * 1024);
		for (int i = 0; i < INT_ARRAY_SIZE; i++) {
			// for (int j = 0; j < 8 * 1024; j++)
			// buffer.putInt(j);
			// q.put(buffer.build());
			// buffer.reset();
			q.put(ba);
			// for (int j = 0; j < 8 * 1024; j++)
			// q.add(IntUtils.intToByteArray(j));
			//
			// os.write(ba);
			// q.put(ba);
		}

		finished.set(true);
		sem.acquire();

		os.close();

		Scanner scanner = new Scanner(res.getIs());
		try {
			while (scanner.hasNext())
				System.out.println(scanner.nextLine());
		} catch (Exception e) {
			e.printStackTrace();
		}
		scanner.close();
		// stream.waitForFinished();
		System.out.println((System.nanoTime() - init) / 1000000);
		cli.close();
	}
}
