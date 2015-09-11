package edu.jlime.jd.sysinfo;

import org.junit.Test;

import edu.jlime.jd.MetricsQuery;
import edu.jlime.jd.client.Client;
import edu.jlime.metrics.metric.Metrics;

public class SysInfoTest {

	@Test
	public void test() {
		try {
			Client client = Client.build();
			Metrics info = client.getCluster().getAnyExecutor().exec(new MetricsQuery());
			// Gson gson = new GsonBuilder().setPrettyPrinting().create();
			// System.out.println(gson.toJson(info));
			client.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
