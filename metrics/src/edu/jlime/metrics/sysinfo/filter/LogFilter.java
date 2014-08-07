package edu.jlime.metrics.sysinfo.filter;

import java.util.HashMap;
import java.util.Map.Entry;

public class LogFilter<T> extends WeightedFilter<T> {

	private static final long serialVersionUID = -7592528392008771291L;

	boolean asc = true;

	public LogFilter(SysInfoFilter<T> f, boolean asc) {
		super(f);
		this.asc = asc;
	}

	@Override
	public HashMap<T, Float> weight(HashMap<T, Float> vals) {

		float x = 0;
		float x2 = 0;
		float max = 0;

		for (Entry<T, Float> val : vals.entrySet()) {
			if (val.getValue() > max)
				max = val.getValue();
		}

		for (Entry<T, Float> val : vals.entrySet()) {
			float normalized = normalize(max, val);
			x2 += normalized * normalized;
			x += normalized;
		}

		int n = vals.size();
		float std_dev = (float) Math.sqrt((x2 / n) - (x / n) * (x / n));

		HashMap<T, Float> res = new HashMap<>();
		for (Entry<T, Float> val : vals.entrySet()) {
			float norm = normalize(max, val);
			float offset = 1 + std_dev;
			float weighted = (float) (Math.log10(norm + 1 + offset) / Math
					.log10(1 + offset));

			res.put(val.getKey(), weighted);
		}
		return res;
	}

	public float normalize(float max, Entry<T, Float> val) {
		return (val.getValue() / max) + (asc ? -1 : 0);
	}
}
