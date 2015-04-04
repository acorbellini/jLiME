package edu.jlime.pregel.graph.rpc;

import java.util.List;
import java.util.Set;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPCClient;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.Transferible;
import gnu.trove.list.array.TLongArrayList;

public class GraphServerImpl extends RPCClient implements Graph, Transferible {

	transient Graph local = null;

	public GraphServerImpl(RPCDispatcher disp, Peer dest, Peer client,
			String targetID) {
		super(disp, dest, client, targetID);
		RPCDispatcher localRPC = RPCDispatcher.getLocalDispatcher(dest);
		if (localRPC != null)
			this.local = (Graph) localRPC.getTarget(targetID);
	}

	public Object get(final long arg0, final String arg1) throws Exception {
		if (local != null) {
			return local.get(arg0, arg1);
		}
		return (Object) disp.callSync(dest, client, targetID, "get",
				new Object[] { arg0, arg1 });
	}

	public double getDouble(final long arg0, final String arg1)
			throws Exception {
		if (local != null) {
			return local.getDouble(arg0, arg1);
		}
		return (double) disp.callSync(dest, client, targetID, "getDouble",
				new Object[] { arg0, arg1 });
	}

	public String getName() throws Exception {
		if (local != null) {
			return local.getName();
		}
		return (String) disp.callSync(dest, client, targetID, "getName",
				new Object[] {});
	}

	public String print() throws Exception {
		if (local != null) {
			return local.print();
		}
		return (String) disp.callSync(dest, client, targetID, "print",
				new Object[] {});
	}

	public void setDouble(final long arg0, final String arg1, final double arg2)
			throws Exception {
		if (local != null) {
			async.execute(new Runnable() {
				public void run() {
					try {
						local.setDouble(arg0, arg1, arg2);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
			;
			return;
		}
		disp.callAsync(dest, client, targetID, "setDouble", new Object[] {
				arg0, arg1, arg2 });
	}

	public Object getDefaultValue(final String arg0) throws Exception {
		if (local != null) {
			return local.getDefaultValue(arg0);
		}
		return (Object) disp.callSync(dest, client, targetID,
				"getDefaultValue", new Object[] { arg0 });
	}

	public void setVal(final long arg0, final String arg1, final Object arg2)
			throws Exception {
		if (local != null) {
			local.setVal(arg0, arg1, arg2);
			return;
		}
		disp.callSync(dest, client, targetID, "setVal", new Object[] { arg0,
				arg1, arg2 });
	}

	public void disable(final long arg0) throws Exception {
		if (local != null) {
			local.disable(arg0);
			return;
		}
		disp.callSync(dest, client, targetID, "disable", new Object[] { arg0 });
	}

	public void removeOutgoing(final long arg0, final long arg1)
			throws Exception {
		if (local != null) {
			local.removeOutgoing(arg0, arg1);
			return;
		}
		disp.callSync(dest, client, targetID, "removeOutgoing", new Object[] {
				arg0, arg1 });
	}

	public int vertexSize() throws Exception {
		if (local != null) {
			return local.vertexSize();
		}
		return (int) disp.callSync(dest, client, targetID, "vertexSize",
				new Object[] {});
	}

	public Iterable vertices() throws Exception {
		if (local != null) {
			return local.vertices();
		}
		return (Iterable) disp.callSync(dest, client, targetID, "vertices",
				new Object[] {});
	}

	public void putLink(final long arg0, final long arg1) throws Exception {
		if (local != null) {
			local.putLink(arg0, arg1);
			return;
		}
		disp.callSync(dest, client, targetID, "putLink", new Object[] { arg0,
				arg1 });
	}

	public void putOutgoing(final List<long[]> arg0) throws Exception {
		if (local != null) {
			local.putOutgoing(arg0);
			return;
		}
		disp.callSync(dest, client, targetID, "putOutgoing",
				new Object[] { arg0 });
	}

	public void putOutgoing(final long arg0, final long arg1) throws Exception {
		if (local != null) {
			local.putOutgoing(arg0, arg1);
			return;
		}
		disp.callSync(dest, client, targetID, "putOutgoing", new Object[] {
				arg0, arg1 });
	}

	public void disableOutgoing(final long arg0, final long arg1)
			throws Exception {
		if (local != null) {
			local.disableOutgoing(arg0, arg1);
			return;
		}
		disp.callSync(dest, client, targetID, "disableOutgoing", new Object[] {
				arg0, arg1 });
	}

	public int getOutgoingSize(final long arg0) throws Exception {
		if (local != null) {
			return local.getOutgoingSize(arg0);
		}
		return (int) disp.callSync(dest, client, targetID, "getOutgoingSize",
				new Object[] { arg0 });
	}

	public void putIncoming(final long arg0, final long arg1) throws Exception {
		if (local != null) {
			local.putIncoming(arg0, arg1);
			return;
		}
		disp.callSync(dest, client, targetID, "putIncoming", new Object[] {
				arg0, arg1 });
	}

	public void createVertices(final Set<java.lang.Long> arg0) throws Exception {
		if (local != null) {
			local.createVertices(arg0);
			return;
		}
		disp.callSync(dest, client, targetID, "createVertices",
				new Object[] { arg0 });
	}

	public int getAdyacencySize(final long arg0) throws Exception {
		if (local != null) {
			return local.getAdyacencySize(arg0);
		}
		return (int) disp.callSync(dest, client, targetID, "getAdyacencySize",
				new Object[] { arg0 });
	}

	public void setDefaultValue(final String arg0, final Object arg1)
			throws Exception {
		if (local != null) {
			local.setDefaultValue(arg0, arg1);
			return;
		}
		disp.callSync(dest, client, targetID, "setDefaultValue", new Object[] {
				arg0, arg1 });
	}

	public void enableAll() throws Exception {
		if (local != null) {
			local.enableAll();
			return;
		}
		disp.callSync(dest, client, targetID, "enableAll", new Object[] {});
	}

	public TLongArrayList getOutgoing(final long arg0) throws Exception {
		if (local != null) {
			return local.getOutgoing(arg0);
		}
		return (TLongArrayList) disp.callSync(dest, client, targetID,
				"getOutgoing", new Object[] { arg0 });
	}

	public TLongArrayList getIncoming(final long arg0) throws Exception {
		if (local != null) {
			return local.getIncoming(arg0);
		}
		return (TLongArrayList) disp.callSync(dest, client, targetID,
				"getIncoming", new Object[] { arg0 });
	}

	public void disableIncoming(final long arg0, final long arg1)
			throws Exception {
		if (local != null) {
			local.disableIncoming(arg0, arg1);
			return;
		}
		disp.callSync(dest, client, targetID, "disableIncoming", new Object[] {
				arg0, arg1 });
	}

	public boolean createVertex(final long arg0) throws Exception {
		if (local != null) {
			return local.createVertex(arg0);
		}
		return (boolean) disp.callSync(dest, client, targetID, "createVertex",
				new Object[] { arg0 });
	}

	public void disableLink(final long arg0, final long arg1) throws Exception {
		if (local != null) {
			local.disableLink(arg0, arg1);
			return;
		}
		disp.callSync(dest, client, targetID, "disableLink", new Object[] {
				arg0, arg1 });
	}

	@Override
	public void setRPC(RPCDispatcher rpc) {
		this.disp = rpc;
	}
}