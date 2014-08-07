package edu.jlime.jd.simpletest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import edu.jlime.client.Client;
import edu.jlime.client.JobContext;
import edu.jlime.jd.JobNode;
import edu.jlime.jd.job.Job;
import edu.jlime.jd.mapreduce.MapReduceTask;

public class SimpleTest {

	public static final class SimpleMR extends
			MapReduceTask<int[], Integer, Integer> {

		private static final long serialVersionUID = 3793991695179624792L;

		public SimpleMR(int[] data) {
			super(data);
		}

		@Override
		public Map<Job<?>, JobNode> map(int[] data, JobContext env) {
			Map<Job<?>, JobNode> res = new HashMap<Job<?>, JobNode>();
			int countData = 0;
			while (countData != data.length) {
				for (JobNode peer : env.getCluster()) {
					res.put(new MyRealJob(data[countData++]), peer);
					if (countData == data.length)
						break;
				}
			}
			return res;
		}

		@Override
		public Integer red(ArrayList<Integer> subres) {
			int sum = 0;
			for (Integer sub : subres)
				sum += sub;
			return sum;
		}
	}

	@Test
	public void test() throws Exception {
		// try {
		// DEFServer server = new DEFServer();
		// } catch (Exception e) {
		// e.printStackTrace();
		// }
		Client cl = Client.build();

		Thread.sleep(3000);

		MapReduceTask<int[], Integer, Integer> task = new SimpleMR(new int[] {
				1, 2, 3, 4, 5, 6, 7, 8 });

		System.out.println(cl.getCluster().getAnyExecutor().exec(task));

		cl.close();

	}
}
