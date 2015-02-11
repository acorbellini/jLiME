package edu.jlime.graphly.traversal;

import edu.jlime.graphly.util.GraphlyUtil;
import gnu.trove.iterator.TLongIntIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongIntHashMap;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class FilterStep implements Step<Object, Object> {

	private static interface Filter<I, F, O> {
		public O filter(I input, F filter);
	}

	private static Map<Class, Filter> filters = new HashMap<Class, Filter>();

	static {
		filters.put(long[].class, new Filter<long[], long[], long[]>() {

			@Override
			public long[] filter(long[] input, long[] filter) {
				TLongArrayList ret = new TLongArrayList();
				for (long l : (long[]) input) {
					if (!GraphlyUtil.in(l, filter)) {
						ret.add(l);
					}
				}
				return ret.toArray();
			}

		});

		filters.put(TLongIntHashMap.class,
				new Filter<TLongIntHashMap, long[], TLongIntHashMap>() {

					@Override
					public TLongIntHashMap filter(TLongIntHashMap input,
							long[] filter) {
						TLongIntIterator it = input.iterator();
						while (it.hasNext()) {
							it.advance();
							if (GraphlyUtil.in(it.key(), filter))
								it.remove();
						}
						return input;
					}

				});
	}
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private long[] filter;
	protected GraphlyTraversal g;

	public FilterStep(long[] first, GraphlyTraversal graphlyTraversal) {
		this.filter = first;
		this.g = graphlyTraversal;
	}

	@Override
	public Object exec(Object before) throws Exception {
		return filter(before, filter);
	}

	protected Object filter(Object before, long[] filter) {
		Arrays.sort(filter);
		Filter f = filters.get(before.getClass());
		if (f == null)
			throw new IllegalStateException("Can't filter " + before.getClass());
		return f.filter(before, filter);
	}
}
