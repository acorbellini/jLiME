package edu.jlime.jd;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.marshalling.TypeConverter;
import edu.jlime.core.marshalling.TypeConverters;
import edu.jlime.util.ByteBuffer;

public class ClientNodeConverter implements TypeConverter {
	private final TypeConverters tc;

	ClientNodeConverter(TypeConverters tc) {
		this.tc = tc;
	}

	@Override
	public void toArray(Object o, ByteBuffer buffer, Peer cliID) throws Exception {
		Node jc = (Node) o;
		tc.objectToByteArray(jc.getPeer(), buffer, cliID);
		tc.objectToByteArray(jc.getClient(), buffer, cliID);
	}

	@Override
	public Object fromArray(ByteBuffer buff) throws Exception {
		Peer p = (Peer) tc.getObjectFromArray(buff);
		Peer client = (Peer) tc.getObjectFromArray(buff);
		Node jn = new Node(p, client, (Dispatcher) DispatcherManager.getJD(tc.getRPC().getCluster().getLocalPeer()));
		return jn;
	}
}