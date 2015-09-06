package edu.jlime.metrics.meters;

import edu.jlime.metrics.metric.Metric;

public class Simple implements Metric<Object>, SimpleMBean {

	private static final long serialVersionUID = 3180566186909423274L;

	private Object val;

	@Override
	public void update(Object val) {
		if (this.val != null && this.val.equals(val))
			return;
		this.val = val;
	}

	@Override
	public String toString() {
		return "val=" + val;
	}

	@Override
	public Object getValue() {
		return val;
	}

	@Override
	public String get() {
		return val.toString();
	}

	@Override
	public Metric<Object> copy() {
		Simple ret = new Simple();
		ret.val = this.val;
		return ret;
	}

}
