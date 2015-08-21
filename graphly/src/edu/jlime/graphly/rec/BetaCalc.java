package edu.jlime.graphly.rec;

import java.io.Serializable;

public interface BetaCalc extends Serializable {
	public float calc(int depth);

	public boolean mustSave(int i);
}
