package edu.jlime.jd.info;

import org.junit.Test;

import edu.jlime.jd.client.Client;
import edu.jlime.jd.profiler.ClusterProfiler;
import edu.jlime.jd.profiler.MetricExtractor;
import edu.jlime.metrics.metric.Metrics;

public class InfoTest {

	@Test
	public void infoTest() throws Exception {
		// DEFServer server1 = new DEFServer();
		// DEFServer server2 = new DEFServer();
		// DEFServer server3 = new DEFServer();
		// DEFServer server4 = new DEFServer();

		Client client = Client.build(8);
		// ClusterInfo info = ClusterInfo.get(client.getCluster());
		ClusterProfiler profiler = new ClusterProfiler(client.getCluster(),
				1000);
		profiler.start();
		Thread.sleep(10000);
		System.out.println(profiler.print(new MetricExtractor() {
			@Override
			public String get(Metrics info) {
				return info.get("sysinfo.mem.used").toString();
			}
		}));

		System.out.println(profiler.print(new MetricExtractor() {

			@Override
			public String get(Metrics info) {
				return info.get("sysinfo.cpu.usage").toString();
			}
		}));

		System.out.println(profiler.print(new MetricExtractor() {
			@Override
			public String get(Metrics info) {
				return info.list("sysinfo.net").findFirst("eth")
						.get("recv_total").toString();
			}
		}));
	}
}
