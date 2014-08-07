package edu.jlime.jd.streaming;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.OutputStreamWriter;
import java.util.Scanner;

import edu.jlime.client.Client;
import edu.jlime.client.JobContext;
import edu.jlime.core.cluster.StreamResult;
import edu.jlime.core.stream.RemoteInputStream;
import edu.jlime.core.stream.RemoteOutputStream;
import edu.jlime.jd.JobNode;
import edu.jlime.jd.job.StreamJob;

public class StreamTest {

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

			DataInputStream reader = new DataInputStream(
					new BufferedInputStream(inputStream, 4096));
			try {
				while (true)
					reader.readInt();
			} catch (EOFException eof) {
				System.out.println("Finished reading");
			} catch (Exception e) {
				e.printStackTrace();
			}

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

		DataOutputStream os = new DataOutputStream(new BufferedOutputStream(
				res.getOs(), 4096));
		for (int i = 0; i < 100000000; i++) {
			os.writeInt(i);
		}
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
