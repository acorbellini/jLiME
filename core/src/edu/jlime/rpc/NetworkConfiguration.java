package edu.jlime.rpc;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

import edu.jlime.rpc.fc.FCConfiguration;
import edu.jlime.rpc.tcp.TCPConfig;

public class NetworkConfiguration {
	Configuration config;

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

	public int ack_delay;

	public int interface_max_update_time;

	public String name;

	public FCConfiguration fcConfig;

	private Properties prop;

	public TCPConfig tcp_config;

	public String protocol;

	public int max_pings;

	public int ping_delay;

	public int tcpnio_max_msg_size;

	public int retransmit_delay;

	public int ack_max_resend_size;

	public String compression;

	public int udp_threads;

	public int nack_delay;

	public int nack_sync_delay;

	public boolean useNACK;

	public int nack_max_resend_size;

	public int nack_resend_delay;

	public float timeout_mult;

	public NetworkConfiguration() {
		this(new Configuration());
	}

	public NetworkConfiguration(Configuration config) {
		this.config = config;

		this.protocol = config.getString("protocol", "tcpnio");

		this.port = config.getInt("port", 3550);
		this.port_range = config.getInt("port_range", 1000);

		this.compression = config.getString("compression", "SNAPPY");

		this.rcvBuffer = config.getInt("udp.rcv_buffer", 25 * 1024 * 1024);
		this.sendBuffer = config.getInt("udp.send_buffer", 25 * 1024 * 1024);
		this.max_msg_size = config.getInt("udp.max_msg_size", 1500);

		this.mcast_addr = config.getString("mcast.addr", "224.0.113.0");
		this.mcastport = config.getInt("mcast.port", 3000);
		this.mcast_port_range = config.getInt("mcast.port_range", 20);

		this.disc_num_tries = config.getInt("disco.tries", 3);
		this.disc_delay = config.getInt("disco.delay", 1000);

		this.max_pings = config.getInt("fd.max_pings", 60);
		this.ping_delay = config.getInt("fd.ping_delay", 1000);

		this.udp_threads = config.getInt("udp.threads", 1);

		this.timeout_mult = config.getFloat("ack-nack.timeout_mult", 3f);

		this.ack_delay = config.getInt("ack.ack_delay", 15);
		this.retransmit_delay = config.getInt("ack.retransmit_delay", 50);
		this.ack_max_resend_size = config.getInt("ack.max_resend_size", 1024);

		this.useNACK = config.getBoolean("ack.usenack", false);

		this.nack_delay = config.getInt("nack.nack_delay", 5);
		this.nack_sync_delay = config.getInt("nack.sync_delay", 15);
		this.nack_resend_delay = config.getInt("nack.resend_delay", 25);
		this.nack_max_resend_size = config.getInt("nack.max_resend_size", 128);

		this.interface_max_update_time = config.getInt("multi.max_update_time", 10000);
		try {
			this.name = config.getString("def.name", InetAddress.getLocalHost().getHostName());
		} catch (UnknownHostException e) {
			this.name = "";
			e.printStackTrace();
		}

		this.fcConfig = new FCConfiguration();
		this.fcConfig.time_before_resend_ack = config.getInt("fc.time_before_resend_ack", 1500);

		this.fcConfig.send_ack_threshold = config.getFloat("fc.fcAckThreshold", 0.9f);
		this.fcConfig.movement_factor = config.getFloat("fc.movement_factor", 0.1f);

		this.fcConfig.old_send_importance = config.getFloat("fc.old_send_importance", 0.7f);
		this.fcConfig.new_send_importance = config.getFloat("fc.new_send_importance", 0.3f);

		this.fcConfig.min_send_threshold = config.getInt("fc.min_rcvd_threshold", 100000);
		this.fcConfig.max_rcv_initial = config.getInt("fc.max_rcv", 100000);
		this.fcConfig.max_send_initial = config.getInt("fc.max_send", 6000);

		this.tcpnio_max_msg_size = config.getInt("tcpnio.max_msg_size", 8 * 1024);

		this.tcp_config = new TCPConfig();
		this.tcp_config.conn_limit = config.getInt("tcp.conn_limit", 2);
		this.tcp_config.time_limit = config.getInt("tcp.time_limit", 15000);
		this.tcp_config.tcp_rcv_buffer = config.getInt("tcp.rcv_buffer", 25 * 1024 * 1024);
		this.tcp_config.tcp_send_buffer = config.getInt("tcp.send_buffer", 25 * 1024 * 1024);
		this.tcp_config.input_buffer = config.getInt("tcp.input_buffer", 8 * 1024);
		this.tcp_config.output_buffer = config.getInt("tcp.output_buffer", 8 * 1024);

	}
}