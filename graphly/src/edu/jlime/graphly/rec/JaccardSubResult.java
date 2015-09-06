package edu.jlime.graphly.rec;

import java.io.Serializable;

import gnu.trove.set.hash.TLongHashSet;

public class JaccardSubResult implements Serializable {
	TLongHashSet union;
	TLongHashSet intersect;
}
