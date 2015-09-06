package edu.jlime.metrics;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map.Entry;

import org.junit.Test;

import edu.jlime.metrics.sysinfo.filter.LogFilter;

public class WeightFilterTest {

	@Test
	public void weightTest() throws Exception {

		int[] data = new int[] { 2, 3, 4, 56, 8, 9, 22, 90, 1, 5, 7 };

		LogFilter filter = new LogFilter(null, true);
		HashMap<String, Float> test = new HashMap<>();
		// test.put(new LocalPeer("a"), 100f);
		// test.put(new LocalPeer("b"), 0f);
		// test.put(new LocalPeer("c"), 0f);

		HashMap<String, Float> res = filter.weight(test);

		float sum = 0;
		for (Float v : res.values()) {
			sum += v;
		}

		// C�digo de divisi�n de trabajos

		int count = 0;
		int init = 0;
		HashMap<String, int[]> div = new HashMap<>();
		for (Entry<String, Float> e : res.entrySet()) {

			count++;

			int end = (int) Math.ceil(init + data.length * e.getValue());

			if (end >= data.length || count == res.size())
				end = data.length;

			int[] dataCopy = Arrays.copyOfRange(data, init, end);
			div.put(e.getKey(), dataCopy);
			init = end;
		}

		System.out.println(sum);
		System.out.println(res);

		for (Entry<String, int[]> e : div.entrySet()) {
			System.out.println(e.getKey() + ", " + e.getValue());
		}
	}
}
