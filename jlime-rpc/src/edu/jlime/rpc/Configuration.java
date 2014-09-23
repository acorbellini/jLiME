package edu.jlime.rpc;

import java.io.FileInputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

import org.apache.log4j.Logger;

import edu.jlime.rpc.fc.FCConfiguration;
import edu.jlime.rpc.tcp.TCPConfig;

public class Configuration {

	public int port;

	public int port_range;

	public int rcvBuffer;

	public int sendBuffer;

	public int max_msg_size;

	public String mcast_addr;

	public int mcastport;

	public int mcast_port_range;

	public int disc_num_tries;

	public long disc_delay;

	public int nack_delay;

	public int ack_delay;

	public int interface_max_update_time;

	public String name;

	public FCConfiguration fcConfig;

	private Properties prop;

	public TCPConfig tcp_config;

	public String protocol;

	public Integer max_pings;

	public Integer ping_delay;

	public Configuration(Properties prop) {

		this.prop = prop;

		this.setProtocol(getString("protocol", "tcp"));

		this.port = getInt("port", 3550);
		this.port_range = getInt("port_range", 1000);

		this.rcvBuffer = getInt("udp.rcv_buffer", 1 * 1024 * 1024);
		this.sendBuffer = getInt("udp.send_buffer", 1 * 1024 * 1024);
		this.max_msg_size = getInt("udp.max_msg_size", 1024);

		this.mcast_addr = getString("mcast.addr", "224.0.113.0");
		this.mcastport = getInt("mcast.port", 3000);
		this.mcast_port_range = getInt("mcast.port_range", 20);

		this.disc_num_tries = getInt("disco.tries", 3);
		this.disc_delay = getInt("disco.delay", 1500);

		this.max_pings = getInt("fd.max_pings", 30);
		this.ping_delay = getInt("fd.ping_delay", 1500);

		this.nack_delay = getInt("ack.nack_delay", 1500);
		this.ack_delay = getInt("ack.ack_delay", 1500);

		this.interface_max_update_time = getInt("multi.max_update_time", 10000);
		try {
			this.name = getString("def.name", InetAddress.getLocalHost()
					.getHostName());
		} catch (UnknownHostException e) {
			this.name = "";
			e.printStackTrace();
		}

		this.fcConfig = new FCConfiguration();
		this.fcConfig.time_before_resend_ack = getInt(
				"fc.time_before_resend_ack", 1500);

		this.fcConfig.send_ack_threshold = getFloat("fc.fcAckThreshold", 0.9f);
		this.fcConfig.movement_factor = getFloat("fc.movement_factor", 0.1f);

		this.fcConfig.old_send_importance = getFloat("fc.old_send_importance",
				0.7f);
		this.fcConfig.new_send_importance = getFloat("fc.new_send_importance",
				0.3f);

		this.fcConfig.min_send_threshold = getInt("fc.min_rcvd_threshold",
				100000);
		this.fcConfig.max_rcv_initial = getInt("fc.max_rcv", 100000);
		this.fcConfig.max_send_initial = getInt("fc.max_send", 6000);

		this.tcp_config = new TCPConfig();
		this.tcp_config.conn_limit = getInt("tcp.conn_limit", 10);
		this.tcp_config.time_limit = getInt("tcp.time_limit", 15000);
		this.tcp_config.tcp_rcv_buffer = getInt("tcp.rcv_buffer",
				1 * 1024 * 1024);
		this.tcp_config.tcp_send_buffer = getInt("tcp.send_buffer",
				1 * 1024 * 1024);
		this.tcp_config.input_buffer = getInt("tcp.input_buffer",
				1 * 1024 * 1024);
		this.tcp_config.output_buffer = getInt("tcp.output_buffer",
				1 * 1024 * 1024);

	}

	public Configuration() {
		this(null);
	}

	private float getFloat(String k, float defaultValue) {
		if (prop != null && prop.getProperty(k) != null)
			try {
				return new Float(prop.getProperty(k));
			} catch (Exception e) {
				e.printStackTrace();
			}
		return defaultValue;
	}

	private String getString(String k, String defaultValue) {
		if (prop != null && prop.getProperty(k) != null)
			try {
				return prop.getProperty(k);
			} catch (Exception e) {
				e.printStackTrace();
			}
		return defaultValue;
	}

	Integer getInt(String k, Integer defaultValue) {
		if (prop != null && prop.getProperty(k) != null)
			try {
				return new Integer(prop.getProperty(k));
			} catch (Exception e) {
				e.printStackTrace();
			}
		return defaultValue;
	}

	public static Configuration newConfig(String propFile) {
		Logger log = Logger.getLogger(Configuration.class);
		Properties prop = new Properties();
		try {
			prop.load(new FileInputStream(propFile));
		} catch (Exception e) {
			log.error("Could not load " + propFile);
			prop = null;
		}
		return new Configuration(prop);
	}

	public String getProtocol() {
		return protocol;
	}

	public void setProtocol(String protocol) {
		this.protocol = protocol;
	}
}