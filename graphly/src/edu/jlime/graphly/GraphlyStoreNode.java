package edu.jlime.graphly;

import edu.jlime.collections.adjacencygraph.get.GetType;
import edu.jlime.collections.intintarray.db.Store;
import edu.jlime.util.DataTypeUtils;
import gnu.trove.list.array.TLongArrayList;

public class GraphlyStoreNode implements GraphlyStoreNodeI {
	Store store;

	public GraphlyStoreNode(Store store) {
		this.store = store;
	}

	/* (non-Javadoc)
	 * @see edu.jlime.graphly.GraphlyStoreNodeI#getEdges(edu.jlime.collections.adjacencygraph.get.GetType, java.lang.Long)
	 */
	@Override
	public long[] getEdges(GetType type, Long id) {

		if (type.equals(GetType.NEIGHBOURS)) {
			TLongArrayList list = new TLongArrayList();
			list.addAll(getEdges0(id));
			list.addAll(getEdges0(-id));
			return list.toArray();
		}
		
		if (type.equals(GetType.FOLLOWEES))
			id = -id;
		
		return getEdges0(id);

	}

	private long[] getEdges0(Long id) {
		byte[] array;
		try {
			array = store.load((int) id.longValue());
			long[] ret = new long[array.length / 4];
			int cont = 0;
			for (int i = 0; i < array.length; i += 4) {
				ret[cont++] = DataTypeUtils.byteArrayToInt(array, i);
			}
			return ret;
		} catch (Exception e) {
			return new long[] {};
		}
	}
}
