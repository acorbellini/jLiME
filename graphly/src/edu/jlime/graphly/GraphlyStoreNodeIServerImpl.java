package edu.jlime.graphly;

import edu.jlime.graphly.GraphlyStoreNodeI;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.RPCClient;
import edu.jlime.core.cluster.Peer;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import edu.jlime.core.rpc.Transferible;
import java.lang.String;
import java.lang.Object;
import java.lang.Exception;
import java.lang.String;
import java.lang.Object;
import java.lang.Exception;
import java.lang.String;
import java.lang.Exception;
import java.lang.String;
import java.lang.Object;
import java.lang.Exception;
import java.lang.String;
import java.util.Map;
import java.lang.Exception;
import java.lang.String;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongObjectHashMap;
import java.lang.Exception;
import java.lang.String;
import gnu.trove.map.hash.TLongObjectHashMap;
import java.lang.Exception;
import java.lang.String;
import java.lang.Exception;
import java.lang.String;
import java.lang.Object;
import java.lang.Exception;
import java.lang.String;
import java.lang.Exception;
import java.util.HashMap;
import java.lang.Exception;
import java.lang.Exception;
import java.lang.String;
import java.lang.Exception;
import java.lang.String;
import java.lang.Exception;
import edu.jlime.graphly.traversal.Dir;
import java.lang.Exception;
import java.lang.String;
import java.lang.Object;
import java.lang.String;
import java.lang.Exception;
import java.lang.Exception;
import java.lang.String;
import java.lang.Exception;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.GraphlyCount;
import java.lang.Exception;
import java.lang.String;
import java.lang.String;
import java.lang.Object;
import java.lang.Exception;
import java.lang.String;
import java.lang.Exception;
import edu.jlime.graphly.traversal.Dir;
import java.lang.Exception;
import java.lang.Exception;
import java.util.List;
import java.lang.Exception;
import edu.jlime.graphly.traversal.Dir;
import java.lang.Exception;
import java.lang.String;
import java.lang.Object;
import java.lang.Exception;
import gnu.trove.list.array.TLongArrayList;
import java.lang.Exception;
import edu.jlime.graphly.traversal.Dir;
import java.lang.Exception;
import java.lang.String;
import java.lang.Exception;

public class GraphlyStoreNodeIServerImpl extends RPCClient implements
		GraphlyStoreNodeI, Transferible {

	transient GraphlyStoreNodeI local = null;

	public GraphlyStoreNodeIServerImpl(RPCDispatcher disp, Peer dest,
			Peer client, String targetID) {
		super(disp, dest, client, targetID);
		RPCDispatcher localRPC = RPCDispatcher.getLocalDispatcher(dest);
		if (localRPC != null)
			this.local = (GraphlyStoreNodeI) localRPC.getTarget(targetID);
	}

	public void setProperty(final long arg0, final String arg1,
			final Object arg2) throws Exception {
		if (local != null) {
			local.setProperty(arg0, arg1, arg2);
			return;
		}
		disp.callSync(dest, client, targetID, "setProperty", new Object[] {
				arg0, arg1, arg2 });
	}

	public Object getProperty(final long arg0, final String arg1)
			throws Exception {
		if (local != null) {
			return local.getProperty(arg0, arg1);
		}
		return (Object) disp.callSync(dest, client, targetID, "getProperty",
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

	public Object getDefault(final String arg0) throws Exception {
		if (local != null) {
			return local.getDefault(arg0);
		}
		return (Object) disp.callSync(dest, client, targetID, "getDefault",
				new Object[] { arg0 });
	}

	public Map getProperties(final long[] arg0, final String[] arg1)
			throws Exception {
		if (local != null) {
			return local.getProperties(arg0, arg1);
		}
		return (Map) disp.callSync(dest, client, targetID, "getProperties",
				new Object[] { arg0, arg1 });
	}

	public TLongObjectHashMap getProperties(final String arg0, final int arg1,
			final TLongArrayList arg2) throws Exception {
		if (local != null) {
			return local.getProperties(arg0, arg1, arg2);
		}
		return (TLongObjectHashMap) disp.callSync(dest, client, targetID,
				"getProperties", new Object[] { arg0, arg1, arg2 });
	}

	public void setProperties(final String arg0,
			final TLongObjectHashMap<java.lang.Object> arg1) throws Exception {
		if (local != null) {
			local.setProperties(arg0, arg1);
			return;
		}
		disp.callSync(dest, client, targetID, "setProperties", new Object[] {
				arg0, arg1 });
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

	public void setDefault(final String arg0, final Object arg1)
			throws Exception {
		if (local != null) {
			local.setDefault(arg0, arg1);
			return;
		}
		disp.callSync(dest, client, targetID, "setDefault", new Object[] {
				arg0, arg1 });
	}

	public void addInEdgePlaceholder(final long arg0, final long arg1,
			final String arg2) throws Exception {
		if (local != null) {
			local.addInEdgePlaceholder(arg0, arg1, arg2);
			return;
		}
		disp.callSync(dest, client, targetID, "addInEdgePlaceholder",
				new Object[] { arg0, arg1, arg2 });
	}

	public void setTempProperties(
			final HashMap<java.lang.Long, java.util.Map<java.lang.String, java.lang.Object>> arg0)
			throws Exception {
		if (local != null) {
			local.setTempProperties(arg0);
			return;
		}
		disp.callSync(dest, client, targetID, "setTempProperties",
				new Object[] { arg0 });
	}

	public void removeVertex(final long arg0) throws Exception {
		if (local != null) {
			local.removeVertex(arg0);
			return;
		}
		disp.callSync(dest, client, targetID, "removeVertex",
				new Object[] { arg0 });
	}

	public double getDefaultDouble(final String arg0) throws Exception {
		if (local != null) {
			return local.getDefaultDouble(arg0);
		}
		return (double) disp.callSync(dest, client, targetID,
				"getDefaultDouble", new Object[] { arg0 });
	}

	public void commitUpdates(final String[] arg0) throws Exception {
		if (local != null) {
			local.commitUpdates(arg0);
			return;
		}
		disp.callSync(dest, client, targetID, "commitUpdates",
				new Object[] { arg0 });
	}

	public long getRandomEdge(final long arg0, final long[] arg1, final Dir arg2)
			throws Exception {
		if (local != null) {
			return local.getRandomEdge(arg0, arg1, arg2);
		}
		return (long) disp.callSync(dest, client, targetID, "getRandomEdge",
				new Object[] { arg0, arg1, arg2 });
	}

	public void setEdgeProperty(final long arg0, final long arg1,
			final String arg2, final Object arg3, final String[] arg4)
			throws Exception {
		if (local != null) {
			local.setEdgeProperty(arg0, arg1, arg2, arg3, arg4);
			return;
		}
		disp.callSync(dest, client, targetID, "setEdgeProperty", new Object[] {
				arg0, arg1, arg2, arg3, arg4 });
	}

	public int getVertexCount() throws Exception {
		if (local != null) {
			return local.getVertexCount();
		}
		return (int) disp.callSync(dest, client, targetID, "getVertexCount",
				new Object[] {});
	}

	public void addVertex(final long arg0, final String arg1) throws Exception {
		if (local != null) {
			async.execute(new Runnable() {
				public void run() {
					try {
						local.addVertex(arg0, arg1);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
			;
			return;
		}
		disp.callAsync(dest, client, targetID, "addVertex", new Object[] {
				arg0, arg1 });
	}

	public GraphlyCount countEdges(final Dir arg0, final int arg1,
			final long[] arg2) throws Exception {
		if (local != null) {
			return local.countEdges(arg0, arg1, arg2);
		}
		return (GraphlyCount) disp.callSync(dest, client, targetID,
				"countEdges", new Object[] { arg0, arg1, arg2 });
	}

	public Object getEdgeProperty(final long arg0, final long arg1,
			final String arg2, final String[] arg3) throws Exception {
		if (local != null) {
			return local.getEdgeProperty(arg0, arg1, arg2, arg3);
		}
		return (Object) disp.callSync(dest, client, targetID,
				"getEdgeProperty", new Object[] { arg0, arg1, arg2, arg3 });
	}

	public String getLabel(final long arg0) throws Exception {
		if (local != null) {
			return local.getLabel(arg0);
		}
		return (String) disp.callSync(dest, client, targetID, "getLabel",
				new Object[] { arg0 });
	}

	public int getEdgeCount(final long arg0, final Dir arg1, final long[] arg2)
			throws Exception {
		if (local != null) {
			return local.getEdgeCount(arg0, arg1, arg2);
		}
		return (int) disp.callSync(dest, client, targetID, "getEdgeCount",
				new Object[] { arg0, arg1, arg2 });
	}

	public void addRange(final int arg0) throws Exception {
		if (local != null) {
			local.addRange(arg0);
			return;
		}
		disp.callSync(dest, client, targetID, "addRange", new Object[] { arg0 });
	}

	public List getRanges() throws Exception {
		if (local != null) {
			return local.getRanges();
		}
		return (List) disp.callSync(dest, client, targetID, "getRanges",
				new Object[] {});
	}

	public void addEdges(final long arg0, final Dir arg1, final long[] arg2)
			throws Exception {
		if (local != null) {
			local.addEdges(arg0, arg1, arg2);
			return;
		}
		disp.callSync(dest, client, targetID, "addEdges", new Object[] { arg0,
				arg1, arg2 });
	}

	public void addEdge(final long arg0, final long arg1, final String arg2,
			final Object[] arg3) throws Exception {
		if (local != null) {
			local.addEdge(arg0, arg1, arg2, arg3);
			return;
		}
		disp.callSync(dest, client, targetID, "addEdge", new Object[] { arg0,
				arg1, arg2, arg3 });
	}

	public TLongArrayList getVertices(final long arg0, final int arg1,
			final boolean arg2) throws Exception {
		if (local != null) {
			return local.getVertices(arg0, arg1, arg2);
		}
		return (TLongArrayList) disp.callSync(dest, client, targetID,
				"getVertices", new Object[] { arg0, arg1, arg2 });
	}

	public long[] getEdges(final Dir arg0, final int arg1, final long[] arg2)
			throws Exception {
		if (local != null) {
			return local.getEdges(arg0, arg1, arg2);
		}
		return (long[]) disp.callSync(dest, client, targetID, "getEdges",
				new Object[] { arg0, arg1, arg2 });
	}

	public void setDefaultDouble(final String arg0, final double arg1)
			throws Exception {
		if (local != null) {
			async.execute(new Runnable() {
				public void run() {
					try {
						local.setDefaultDouble(arg0, arg1);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
			;
			return;
		}
		disp.callAsync(dest, client, targetID, "setDefaultDouble",
				new Object[] { arg0, arg1 });
	}

	@Override
	public void setRPC(RPCDispatcher rpc) {
		this.disp = rpc;
	}
}