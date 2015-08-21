package edu.jlime.graphly.rec;

import gnu.trove.set.hash.TLongHashSet;

import java.io.Serializable;

public class JaccardSubResult implements Serializable {
	TLongHashSet union;
	TLongHashSet intersect;
}
