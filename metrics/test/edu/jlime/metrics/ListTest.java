package edu.jlime.metrics;

import edu.jlime.metrics.metric.Metrics;

public class ListTest {
	public static void main(String[] args) {
		Metrics m = new Metrics("test");
		m.simple("sysinfo.net.eth0.sent_total", "1");
		m.simple("sysinfo.net.eth0.rcvd_total", "2");
		m.simple("sysinfo.net.eth0.ip", "3");
		m.simple("sysinfo.net.wlan.sent_total", "0");
		m.simple("sysinfo.net.wlan.rcvd_total", "0");
		m.simple("sysinfo.net.wlan.ip", "0");
		m.simple("sysinfo.net.wlan.signal", "0");
		m.simple("sysinfo.net.z.what", "0");

		System.out.println(m.list("sysinfo.net").findFirst("eth").get("sent_total"));
	}
}
