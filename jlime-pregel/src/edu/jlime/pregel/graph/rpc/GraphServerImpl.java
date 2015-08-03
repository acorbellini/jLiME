package edu.jlime.pregel.graph.rpc;

import java.util.List;
import java.util.Set;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPCClient;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.Transferible;
import gnu.trove.list.array.TLongArrayList;

public class GraphServerImpl extends RPCClient implements Graph, Transferible {

	transient RPCDispatcher localRPC;
	transient volatile Graph local = null;

	public GraphServerImpl(RPCDispatcher disp, Peer dest, Peer client,
			String targetID) {
		super(disp, dest, client, targetID);
		this.localRPC = RPCDispatcher.getLocalDispatcher(dest);
	}

	public Object get(final long arg0, final String arg1) throws Exception {
		if (localRPC != null) {
			return getLocal().get(arg0, arg1);
		}
		return (Object) disp.callSync(dest, client, targetID, "get",
				new Object[] { arg0, arg1 });
	}

	public float getFloat(final long arg0, final String arg1) throws Exception {
		if (localRPC != null) {
			return getLocal().getFloat(arg0, arg1);
		}
		return (float) disp.callSync(dest, client, targetID, "getFloat",
				new Object[] { arg0, arg1 });
	}

	public double getDouble(final long arg0, final String arg1)
			throws Exception {
		if (localRPC != null) {
			return getLocal().getDouble(arg0, arg1);
		}
		return (double) disp.callSync(dest, client, targetID, "getDouble",
				new Object[] { arg0, arg1 });
	}

	public String getName() throws Exception {
		if (localRPC != null) {
			return getLocal().getName();
		}
		return (String) disp.callSync(dest, client, targetID, "getName",
				new Object[] {});
	}

	public String print() throws Exception {
		if (localRPC != null) {
			return getLocal().print();
		}
		return (String) disp.callSync(dest, client, targetID, "print",
				new Object[] {});
	}

	public void setDouble(final long arg0, final String arg1, final double arg2)
			throws Exception {
		if (localRPC != null) {
			getLocal().setDouble(arg0, arg1, arg2);
			return;
		}
		disp.callSync(dest, client, targetID, "setDouble", new Object[] { arg0,
				arg1, arg2 });
	}

	public void setFloat(final long arg0, final String arg1, final float arg2)
			throws Exception {
		if (localRPC != null) {
			getLocal().setFloat(arg0, arg1, arg2);
			return;
		}
		disp.callSync(dest, client, targetID, "setFloat", new Object[] { arg0,
				arg1, arg2 });
	}

	public Object getDefaultValue(final String arg0) throws Exception {
		if (localRPC != null) {
			return getLocal().getDefaultValue(arg0);
		}
		return (Object) disp.callSync(dest, client, targetID,
				"getDefaultValue", new Object[] { arg0 });
	}

	public void setVal(final long arg0, final String arg1, final Object arg2)
			throws Exception {
		if (localRPC != null) {
			getLocal().setVal(arg0, arg1, arg2);
			return;
		}
		disp.callSync(dest, client, targetID, "setVal", new Object[] { arg0,
				arg1, arg2 });
	}

	public void disable(final long arg0) throws Exception {
		if (localRPC != null) {
			getLocal().disable(arg0);
			return;
		}
		disp.callSync(dest, client, targetID, "disable", new Object[] { arg0 });
	}

	public int getAdyacencySize(final long arg0) throws Exception {
		if (localRPC != null) {
			return getLocal().getAdyacencySize(arg0);
		}
		return (int) disp.callSync(dest, client, targetID, "getAdyacencySize",
				new Object[] { arg0 });
	}

	public void putOutgoing(final long arg0, final long arg1) throws Exception {
		if (localRPC != null) {
			getLocal().putOutgoing(arg0, arg1);
			return;
		}
		disp.callSync(dest, client, targetID, "putOutgoing", new Object[] {
				arg0, arg1 });
	}

	public void putOutgoing(final List<long[]> arg0) throws Exception {
		if (localRPC != null) {
			getLocal().putOutgoing(arg0);
			return;
		}
		disp.callSync(dest, client, targetID, "putOutgoing",
				new Object[] { arg0 });
	}

	public int getOutgoingSize(final long arg0) throws Exception {
		if (localRPC != null) {
			return getLocal().getOutgoingSize(arg0);
		}
		return (int) disp.callSync(dest, client, targetID, "getOutgoingSize",
				new Object[] { arg0 });
	}

	public void disableLink(final long arg0, final long arg1) throws Exception {
		if (localRPC != null) {
			getLocal().disableLink(arg0, arg1);
			return;
		}
		disp.callSync(dest, client, targetID, "disableLink", new Object[] {
				arg0, arg1 });
	}

	public void disableIncoming(final long arg0, final long arg1)
			throws Exception {
		if (localRPC != null) {
			getLocal().disableIncoming(arg0, arg1);
			return;
		}
		disp.callSync(dest, client, targetID, "disableIncoming", new Object[] {
				arg0, arg1 });
	}

	public void createVertices(final Set<java.lang.Long> arg0) throws Exception {
		if (localRPC != null) {
			getLocal().createVertices(arg0);
			return;
		}
		disp.callSync(dest, client, targetID, "createVertices",
				new Object[] { arg0 });
	}

	public void putLink(final long arg0, final long arg1) throws Exception {
		if (localRPC != null) {
			getLocal().putLink(arg0, arg1);
			return;
		}
		disp.callSync(dest, client, targetID, "putLink", new Object[] { arg0,
				arg1 });
	}

	public void setDefaultValue(final String arg0, final Object arg1)
			throws Exception {
		if (localRPC != null) {
			getLocal().setDefaultValue(arg0, arg1);
			return;
		}
		disp.callSync(dest, client, targetID, "setDefaultValue", new Object[] {
				arg0, arg1 });
	}

	public void enableAll() throws Exception {
		if (localRPC != null) {
			getLocal().enableAll();
			return;
		}
		disp.callSync(dest, client, targetID, "enableAll", new Object[] {});
	}

	public void disableOutgoing(final long arg0, final long arg1)
			throws Exception {
		if (localRPC != null) {
			getLocal().disableOutgoing(arg0, arg1);
			return;
		}
		disp.callSync(dest, client, targetID, "disableOutgoing", new Object[] {
				arg0, arg1 });
	}

	public boolean createVertex(final long arg0) throws Exception {
		if (localRPC != null) {
			return getLocal().createVertex(arg0);
		}
		return (boolean) disp.callSync(dest, client, targetID, "createVertex",
				new Object[] { arg0 });
	}

	public TLongArrayList getOutgoing(final long arg0) throws Exception {
		if (localRPC != null) {
			return getLocal().getOutgoing(arg0);
		}
		return (TLongArrayList) disp.callSync(dest, client, targetID,
				"getOutgoing", new Object[] { arg0 });
	}

	public Iterable vertices() throws Exception {
		if (localRPC != null) {
			return getLocal().vertices();
		}
		return (Iterable) disp.callSync(dest, client, targetID, "vertices",
				new Object[] {});
	}

	public void removeOutgoing(final long arg0, final long arg1)
			throws Exception {
		if (localRPC != null) {
			getLocal().removeOutgoing(arg0, arg1);
			return;
		}
		disp.callSync(dest, client, targetID, "removeOutgoing", new Object[] {
				arg0, arg1 });
	}

	public float getDefaultFloat(final String arg0) throws Exception {
		if (localRPC != null) {
			return getLocal().getDefaultFloat(arg0);
		}
		return (float) disp.callSync(dest, client, targetID, "getDefaultFloat",
				new Object[] { arg0 });
	}

	public TLongArrayList getIncoming(final long arg0) throws Exception {
		if (localRPC != null) {
			return getLocal().getIncoming(arg0);
		}
		return (TLongArrayList) disp.callSync(dest, client, targetID,
				"getIncoming", new Object[] { arg0 });
	}

	public void putIncoming(final long arg0, final long arg1) throws Exception {
		if (localRPC != null) {
			getLocal().putIncoming(arg0, arg1);
			return;
		}
		disp.callSync(dest, client, targetID, "putIncoming", new Object[] {
				arg0, arg1 });
	}

	public int vertexSize() throws Exception {
		if (localRPC != null) {
			return getLocal().vertexSize();
		}
		return (int) disp.callSync(dest, client, targetID, "vertexSize",
				new Object[] {});
	}

	@Override
	public void setRPC(RPCDispatcher rpc) {
		this.disp = rpc;
		this.localRPC = RPCDispatcher.getLocalDispatcher(super.dest);
	}

	public Graph getLocal() throws Exception {
		if (local == null) {
			synchronized (this) {
				if (local == null) {
					this.local = (Graph) localRPC.getTarget(targetID);
				}
			}
		}
		return this.local;
	}
}