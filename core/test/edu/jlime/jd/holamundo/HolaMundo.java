package edu.jlime.jd.holamundo;

import edu.jlime.jd.Node;
import edu.jlime.jd.client.Client;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.RunJob;

public class HolaMundo extends RunJob {

	int[] data;

	public HolaMundo() {
		data = new int[1500000];
		for (int i = 0; i < data.length; i++) {
			data[i] = (int) (Math.random() * 10000);
		}
	}

	@Override
	public void run(JobContext env, Node origin) throws Exception {
		System.out.println("Hola fucking mundo!");
	}

	public static void main(String[] args) throws Exception {
		Client cli = Client.build(1);
		cli.getCluster().broadcast(new HolaMundo());
		cli.close();
	}

}