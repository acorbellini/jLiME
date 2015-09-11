package edu.jlime.graphly.storenode.rpc;

import java.util.HashMap;
import java.util.List;
import java.util.List;
import java.util.Map;
import java.util.Map;
import java.util.Set;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPCClient;
import edu.jlime.core.rpc.RPC;
import edu.jlime.core.rpc.Transferible;
import edu.jlime.graphly.rec.hits.DivideUpdateProperty;
import edu.jlime.graphly.storenode.Count;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.util.Gather;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;

public class GraphlyStoreNodeIServerImpl extends RPCClient implements StoreNode, Transferible {

	transient RPC localRPC;
	transient volatile StoreNode local = null;

	public GraphlyStoreNodeIServerImpl(RPC disp, Peer dest, Peer client, String targetID) {
		super(disp, dest, client, targetID);
		this.localRPC = RPC.getLocalDispatcher(dest);
	}

	public void setProperty(final String arg0, final String arg1, final String arg2, final TLongArrayList arg3)
			throws Exception {
		if (localRPC != null) {
			getLocal().setProperty(arg0, arg1, arg2, arg3);
			return;
		}
		disp.callSync(dest, client, targetID, "setProperty", new Object[] { arg0, arg1, arg2, arg3 });
	}

	public void setProperty(final String arg0, final long arg1, final String arg2, final Object arg3) throws Exception {
		if (localRPC != null) {
			getLocal().setProperty(arg0, arg1, arg2, arg3);
			return;
		}
		disp.callSync(dest, client, targetID, "setProperty", new Object[] { arg0, arg1, arg2, arg3 });
	}

	public Object getProperty(final String arg0, final long arg1, final String arg2) throws Exception {
		if (localRPC != null) {
			return getLocal().getProperty(arg0, arg1, arg2);
		}
		return (Object) disp.callSync(dest, client, targetID, "getProperty", new Object[] { arg0, arg1, arg2 });
	}

	public float getFloat(final String arg0, final long arg1, final String arg2, final float arg3) throws Exception {
		if (localRPC != null) {
			return getLocal().getFloat(arg0, arg1, arg2, arg3);
		}
		return (float) disp.callSync(dest, client, targetID, "getFloat", new Object[] { arg0, arg1, arg2, arg3 });
	}

	public float getFloat(final String arg0, final long arg1, final String arg2) throws Exception {
		if (localRPC != null) {
			return getLocal().getFloat(arg0, arg1, arg2);
		}
		return (float) disp.callSync(dest, client, targetID, "getFloat", new Object[] { arg0, arg1, arg2 });
	}

	public double getDouble(final String arg0, final long arg1, final String arg2) throws Exception {
		if (localRPC != null) {
			return getLocal().getDouble(arg0, arg1, arg2);
		}
		return (double) disp.callSync(dest, client, targetID, "getDouble", new Object[] { arg0, arg1, arg2 });
	}

	public Object getDefault(final String arg0, final String arg1) throws Exception {
		if (localRPC != null) {
			return getLocal().getDefault(arg0, arg1);
		}
		return (Object) disp.callSync(dest, client, targetID, "getDefault", new Object[] { arg0, arg1 });
	}

	public Map getProperties(final String arg0, final long[] arg1, final String[] arg2) throws Exception {
		if (localRPC != null) {
			return getLocal().getProperties(arg0, arg1, arg2);
		}
		return (Map) disp.callSync(dest, client, targetID, "getProperties", new Object[] { arg0, arg1, arg2 });
	}

	public TLongObjectHashMap getProperties(final String arg0, final String arg1, final int arg2,
			final TLongArrayList arg3) throws Exception {
		if (localRPC != null) {
			return getLocal().getProperties(arg0, arg1, arg2, arg3);
		}
		return (TLongObjectHashMap) disp.callSync(dest, client, targetID, "getProperties",
				new Object[] { arg0, arg1, arg2, arg3 });
	}

	public void setProperties(final String arg0, final String arg1, final TLongObjectHashMap<java.lang.Object> arg2)
			throws Exception {
		if (localRPC != null) {
			getLocal().setProperties(arg0, arg1, arg2);
			return;
		}
		disp.callSync(dest, client, targetID, "setProperties", new Object[] { arg0, arg1, arg2 });
	}

	public void setDouble(final String arg0, final long arg1, final String arg2, final double arg3) throws Exception {
		if (localRPC != null) {
			getLocal().setDouble(arg0, arg1, arg2, arg3);
			return;
		}
		disp.callSync(dest, client, targetID, "setDouble", new Object[] { arg0, arg1, arg2, arg3 });
	}

	public void setFloat(final String arg0, final long arg1, final String arg2, final float arg3) throws Exception {
		if (localRPC != null) {
			getLocal().setFloat(arg0, arg1, arg2, arg3);
			return;
		}
		disp.callSync(dest, client, targetID, "setFloat", new Object[] { arg0, arg1, arg2, arg3 });
	}

	public void setDefault(final String arg0, final String arg1, final Object arg2) throws Exception {
		if (localRPC != null) {
			getLocal().setDefault(arg0, arg1, arg2);
			return;
		}
		disp.callSync(dest, client, targetID, "setDefault", new Object[] { arg0, arg1, arg2 });
	}

	public void addInEdgePlaceholder(final String arg0, final long arg1, final long arg2, final String arg3)
			throws Exception {
		if (localRPC != null) {
			getLocal().addInEdgePlaceholder(arg0, arg1, arg2, arg3);
			return;
		}
		disp.callSync(dest, client, targetID, "addInEdgePlaceholder", new Object[] { arg0, arg1, arg2, arg3 });
	}

	public void commitFloatUpdates(final String arg0, final String[] arg1) throws Exception {
		if (localRPC != null) {
			getLocal().commitFloatUpdates(arg0, arg1);
			return;
		}
		disp.callSync(dest, client, targetID, "commitFloatUpdates", new Object[] { arg0, arg1 });
	}

	public void setTempProperties(final String arg0,
			final HashMap<java.lang.Long, java.util.Map<java.lang.String, java.lang.Object>> arg1) throws Exception {
		if (localRPC != null) {
			getLocal().setTempProperties(arg0, arg1);
			return;
		}
		disp.callSync(dest, client, targetID, "setTempProperties", new Object[] { arg0, arg1 });
	}

	public void updateFloatProperty(final String arg0, final String arg1, final DivideUpdateProperty arg2)
			throws Exception {
		if (localRPC != null) {
			getLocal().updateFloatProperty(arg0, arg1, arg2);
			return;
		}
		disp.callSync(dest, client, targetID, "updateFloatProperty", new Object[] { arg0, arg1, arg2 });
	}

	public void removeVertex(final String arg0, final long arg1) throws Exception {
		if (localRPC != null) {
			getLocal().removeVertex(arg0, arg1);
			return;
		}
		disp.callSync(dest, client, targetID, "removeVertex", new Object[] { arg0, arg1 });
	}

	public int getEdgeCount(final String arg0, final long arg1, final Dir arg2, final TLongHashSet arg3)
			throws Exception {
		if (localRPC != null) {
			return getLocal().getEdgeCount(arg0, arg1, arg2, arg3);
		}
		return (int) disp.callSync(dest, client, targetID, "getEdgeCount", new Object[] { arg0, arg1, arg2, arg3 });
	}

	public void commitUpdates(final String arg0, final String[] arg1) throws Exception {
		if (localRPC != null) {
			getLocal().commitUpdates(arg0, arg1);
			return;
		}
		disp.callSync(dest, client, targetID, "commitUpdates", new Object[] { arg0, arg1 });
	}

	public void setDefaultFloat(final String arg0, final String arg1, final float arg2) throws Exception {
		if (localRPC != null) {
			getLocal().setDefaultFloat(arg0, arg1, arg2);
			return;
		}
		disp.callSync(dest, client, targetID, "setDefaultFloat", new Object[] { arg0, arg1, arg2 });
	}

	public Object getEdgeProperty(final String arg0, final long arg1, final long arg2, final String arg3,
			final String[] arg4) throws Exception {
		if (localRPC != null) {
			return getLocal().getEdgeProperty(arg0, arg1, arg2, arg3, arg4);
		}
		return (Object) disp.callSync(dest, client, targetID, "getEdgeProperty",
				new Object[] { arg0, arg1, arg2, arg3, arg4 });
	}

	public void addVertex(final String arg0, final long arg1, final String arg2) throws Exception {
		if (localRPC != null) {
			async.execute(new Runnable() {
				public void run() {
					try {
						getLocal().addVertex(arg0, arg1, arg2);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
			;
			return;
		}
		disp.callAsync(dest, client, targetID, "addVertex", new Object[] { arg0, arg1, arg2 });
	}

	public void addEdge(final String arg0, final long arg1, final long arg2, final String arg3, final Object[] arg4)
			throws Exception {
		if (localRPC != null) {
			getLocal().addEdge(arg0, arg1, arg2, arg3, arg4);
			return;
		}
		disp.callSync(dest, client, targetID, "addEdge", new Object[] { arg0, arg1, arg2, arg3, arg4 });
	}

	public String getLabel(final String arg0, final long arg1) throws Exception {
		if (localRPC != null) {
			return getLocal().getLabel(arg0, arg1);
		}
		return (String) disp.callSync(dest, client, targetID, "getLabel", new Object[] { arg0, arg1 });
	}

	public long[] getEdges(final String arg0, final Dir arg1, final int arg2, final long[] arg3) throws Exception {
		if (localRPC != null) {
			return getLocal().getEdges(arg0, arg1, arg2, arg3);
		}
		return (long[]) disp.callSync(dest, client, targetID, "getEdges", new Object[] { arg0, arg1, arg2, arg3 });
	}

	public Count countEdges(final String arg0, final Dir arg1, final int arg2, final long[] arg3, final float[] arg4,
			final long[] arg5) throws Exception {
		if (localRPC != null) {
			return getLocal().countEdges(arg0, arg1, arg2, arg3, arg4, arg5);
		}
		return (Count) disp.callSync(dest, client, targetID, "countEdges",
				new Object[] { arg0, arg1, arg2, arg3, arg4, arg5 });
	}

	public List getRanges() throws Exception {
		if (localRPC != null) {
			return getLocal().getRanges();
		}
		return (List) disp.callSync(dest, client, targetID, "getRanges", new Object[] {});
	}

	public void setEdgeProperty(final String arg0, final long arg1, final long arg2, final String arg3,
			final Object arg4, final String[] arg5) throws Exception {
		if (localRPC != null) {
			getLocal().setEdgeProperty(arg0, arg1, arg2, arg3, arg4, arg5);
			return;
		}
		disp.callSync(dest, client, targetID, "setEdgeProperty", new Object[] { arg0, arg1, arg2, arg3, arg4, arg5 });
	}

	public long getRandomEdge(final String arg0, final long arg1, final long[] arg2, final Dir arg3) throws Exception {
		if (localRPC != null) {
			return getLocal().getRandomEdge(arg0, arg1, arg2, arg3);
		}
		return (long) disp.callSync(dest, client, targetID, "getRandomEdge", new Object[] { arg0, arg1, arg2, arg3 });
	}

	public int getVertexCount(final String arg0) throws Exception {
		if (localRPC != null) {
			return getLocal().getVertexCount(arg0);
		}
		return (int) disp.callSync(dest, client, targetID, "getVertexCount", new Object[] { arg0 });
	}

	public TLongArrayList getVertices(final String arg0, final long arg1, final int arg2, final boolean arg3)
			throws Exception {
		if (localRPC != null) {
			return getLocal().getVertices(arg0, arg1, arg2, arg3);
		}
		return (TLongArrayList) disp.callSync(dest, client, targetID, "getVertices",
				new Object[] { arg0, arg1, arg2, arg3 });
	}

	public void setDefaultDouble(final String arg0, final String arg1, final double arg2) throws Exception {
		if (localRPC != null) {
			getLocal().setDefaultDouble(arg0, arg1, arg2);
			return;
		}
		disp.callSync(dest, client, targetID, "setDefaultDouble", new Object[] { arg0, arg1, arg2 });
	}

	public double getDefaultDouble(final String arg0, final String arg1) throws Exception {
		if (localRPC != null) {
			return getLocal().getDefaultDouble(arg0, arg1);
		}
		return (double) disp.callSync(dest, client, targetID, "getDefaultDouble", new Object[] { arg0, arg1 });
	}

	public void addEdges(final String arg0, final long arg1, final Dir arg2, final long[] arg3) throws Exception {
		if (localRPC != null) {
			getLocal().addEdges(arg0, arg1, arg2, arg3);
			return;
		}
		disp.callSync(dest, client, targetID, "addEdges", new Object[] { arg0, arg1, arg2, arg3 });
	}

	public Set getGraphs() throws Exception {
		if (localRPC != null) {
			return getLocal().getGraphs();
		}
		return (Set) disp.callSync(dest, client, targetID, "getGraphs", new Object[] {});
	}

	public float getDefaultFloat(final String arg0, final String arg1) throws Exception {
		if (localRPC != null) {
			return getLocal().getDefaultFloat(arg0, arg1);
		}
		return (float) disp.callSync(dest, client, targetID, "getDefaultFloat", new Object[] { arg0, arg1 });
	}

	public Object gather(final String arg0, final Gather<?> arg1) throws Exception {
		if (localRPC != null) {
			return getLocal().gather(arg0, arg1);
		}
		return (Object) disp.callSync(dest, client, targetID, "gather", new Object[] { arg0, arg1 });
	}

	public void setTempFloats(final String arg0, final String arg1, final boolean arg2, final TLongFloatHashMap arg3)
			throws Exception {
		if (localRPC != null) {
			getLocal().setTempFloats(arg0, arg1, arg2, arg3);
			return;
		}
		disp.callSync(dest, client, targetID, "setTempFloats", new Object[] { arg0, arg1, arg2, arg3 });
	}

	public void setFloats(final String arg0, final String arg1, final TLongFloatHashMap arg2) throws Exception {
		if (localRPC != null) {
			getLocal().setFloats(arg0, arg1, arg2);
			return;
		}
		disp.callSync(dest, client, targetID, "setFloats", new Object[] { arg0, arg1, arg2 });
	}

	public void addRange(final int arg0) throws Exception {
		if (localRPC != null) {
			getLocal().addRange(arg0);
			return;
		}
		disp.callSync(dest, client, targetID, "addRange", new Object[] { arg0 });
	}

	@Override
	public void setRPC(RPC rpc) {
		this.disp = rpc;
		this.localRPC = RPC.getLocalDispatcher(super.dest);
	}

	public StoreNode getLocal() throws Exception {
		if (local == null) {
			synchronized (this) {
				if (local == null) {
					this.local = (StoreNode) localRPC.getTarget(targetID);
				}
			}
		}
		return this.local;
	}
}