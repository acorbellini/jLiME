package edu.jlime.jd.client;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;

import edu.jlime.core.cluster.DataFilter;
import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPC;
import edu.jlime.core.stream.RemoteInputStream;
import edu.jlime.core.stream.RemoteOutputStream;
import edu.jlime.jd.ClientCluster;
import edu.jlime.jd.Dispatcher;
import edu.jlime.jd.StreamProvider;
import edu.jlime.rpc.JLiMEFactory;
import edu.jlime.rpc.NetworkConfiguration;

public class Client implements Closeable {

	Dispatcher jd;

	public Client(Dispatcher jd) throws Exception {
		this.jd = jd;
	}

	public static Client build() throws Exception {
		return build(0);
	}

	public static Client build(int i) throws Exception {
		HashMap<String, String> jdData = new HashMap<>();
		jdData.put("app", "jobdispatcher");
		jdData.put(Dispatcher.ISEXEC, Boolean.valueOf(false).toString());
		jdData.put(Dispatcher.TAGS, "Client");

		NetworkConfiguration config = new NetworkConfiguration();

		final RPC rpc = new JLiMEFactory(config, jdData, new DataFilter("app", "job", true)).build();

		Dispatcher jd = new Dispatcher(i, rpc);
		jd.setStreamer(new StreamProvider() {

			@Override
			public RemoteOutputStream getOutputStream(UUID streamID, Peer to) {
				return rpc.getStreamer().getOutputStream(streamID, to.getAddress());
			}

			@Override
			public RemoteInputStream getInputStream(UUID streamID, Peer to) {
				return rpc.getStreamer().getInputStream(streamID, to.getAddress());
			}
		});
		jd.start();
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

	public Dispatcher getJd() {
		return jd;
	}

}
