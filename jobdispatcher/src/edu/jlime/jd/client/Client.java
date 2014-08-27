package edu.jlime.jd.client;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.stream.RemoteInputStream;
import edu.jlime.core.stream.RemoteOutputStream;
import edu.jlime.jd.ClientCluster;
import edu.jlime.jd.JobDispatcher;
import edu.jlime.jd.StreamProvider;
import edu.jlime.rpc.Configuration;
import edu.jlime.rpc.JlimeFactory;

public class Client implements Closeable {

	JobDispatcher jd;

	public Client(JobDispatcher jd) throws Exception {
		this.jd = jd;
		jd.start();
	}

	public static Client build() throws Exception {
		return build(0);
	}

	public static Client build(int i) throws Exception {
		HashMap<String, String> jdData = new HashMap<>();
		jdData.put(JobDispatcher.ISEXEC, Boolean.valueOf(false).toString());
		jdData.put(JobDispatcher.TAGS, "Client");

		Configuration config = new Configuration();
		// config.port = 3552;
		// config.port_range = 1;
		final RPCDispatcher rpc = new JlimeFactory(config, jdData).build();
		JobDispatcher jd = new JobDispatcher(i, rpc);
		jd.setStreamer(new StreamProvider() {

			@Override
			public RemoteOutputStream getOutputStream(UUID streamID, Peer to) {
				return rpc.getStreamer().getOutputStream(streamID,
						to.getAddress());
			}

			@Override
			public RemoteInputStream getInputStream(UUID streamID, Peer to) {
				return rpc.getStreamer().getInputStream(streamID,
						to.getAddress());
			}
		});
		return new Client(jd);
	}

	public ClientCluster getCluster() {
		return jd.getCluster();
	}

	@Override
	public void close() throws IOException {
		try {
			jd.stop();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
