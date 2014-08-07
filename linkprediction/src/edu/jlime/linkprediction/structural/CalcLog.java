package edu.jlime.linkprediction.structural;

import edu.jlime.collections.adjacencygraph.query.ForEachQueryProc;
import edu.jlime.collections.adjacencygraph.query.ListQuery;
import edu.jlime.collections.adjacencygraph.query.RemoteQuery;

public class CalcLog implements ForEachQueryProc<Float> {

	private static final long serialVersionUID = 9071313131034167954L;

	@Override
	public Float call(ListQuery userId) throws Exception {
		int neighbourSize = ((RemoteQuery<Integer>) userId.neighbours().size())
				.query();
		float logVal = 0;
		if (neighbourSize > 1)
			logVal = (float) (1 / Math.log10(neighbourSize));
		else
			logVal = 3.32192809489f;// max value (equals to neighbours ==2)
		return logVal;
	}
}