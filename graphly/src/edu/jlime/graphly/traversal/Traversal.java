package edu.jlime.graphly.traversal;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import edu.jlime.graphly.client.Graph;
import edu.jlime.graphly.rec.Beta;
import edu.jlime.graphly.rec.CustomStep;
import edu.jlime.graphly.rec.CustomStep.CustomFunction;
import edu.jlime.graphly.rec.Repeat;
import edu.jlime.graphly.rec.VertexFilter;
import edu.jlime.graphly.traversal.RepeatStep.RepeatSync;
import edu.jlime.graphly.traversal.count.CountStep;
import edu.jlime.graphly.traversal.each.EachStep;
import edu.jlime.graphly.traversal.each.ForEach;
import edu.jlime.jd.Node;
import gnu.trove.set.hash.TLongHashSet;

public class Traversal implements Serializable {

	Map<String, Object> vars = new HashMap<>();

	List<Step> steps = new ArrayList<>();

	int curr = 0;

	private TraversalResult currres;

	private Graph g;

	private boolean printSteps = false;
	private long lastexec = -1;

	private String graphID;

	public Traversal(long[] ids, Graph g) {
		this.currres = new VertexResult(ids);
		this.g = g;
	}

	public Traversal(TLongHashSet ids, Graph graph) {
		this.currres = new VertexResult(ids);
		this.g = graph;
	}

	public Traversal setPrintSteps(boolean printSteps) {
		this.printSteps = printSteps;
		return this;
	}

	public TraversalResult submit(Node c) throws Exception {
		return c.exec(new TraversalJob(this));
	}

	public TraversalResult next() throws Exception {
		Step step = steps.get(curr++);
		if (printSteps) {
			Logger log = Logger.getLogger(Traversal.class);
			if (lastexec < 0)
				lastexec = System.currentTimeMillis();

			log.info("Executing step " + curr + "(" + (System.currentTimeMillis() - lastexec) / 1000 + " sec. ):"
					+ step.toString());
		}
		this.currres = step.exec(currres);
		return this.currres;
	}

	public TraversalResult exec() throws Exception {
		for (int i = curr; i < steps.size(); i++) {
			next();
		}
		return this.currres;
	}

	public Traversal out() {
		return to(Dir.OUT);
	}

	public Traversal to(Dir dir, int max_edges) {
		addStep(new VertexStep(dir, max_edges, this));
		return this;
	}

	public Traversal addStep(Step vertexStep) {
		steps.add(vertexStep);
		return this;
	}

	public Object get(String k) {
		return vars.get(k);
	}

	public Traversal set(String k, Object v) {
		vars.put(k, v);
		return this;
	}

	public Traversal in() {
		return to(Dir.IN);
	}

	public Graph getGraph() {
		return g;
	}

	public Traversal filter(long[] first) {
		addStep(new FilterStep(first, this));
		return this;
	}

	public Traversal count(Dir dir, int max_edges, String[] filters) {
		// addStep(new ParallelStep(new CountStep(dir, max_edges, this), this,
		// 2,
		// new CountMerger()));
		addStep(new CountStep(dir, max_edges, filters, this));
		return this;
	}

	public Traversal countOut() {
		return countOut(Integer.MAX_VALUE);
	}

	public Traversal countOut(int max_edges) {
		return count(Dir.OUT, max_edges);
	}

	private Traversal count(Dir dir, int max_edges) {
		return this.count(dir, max_edges, new String[] {});
	}

	public <T extends CustomTraversal> T as(Class<T> c) throws Exception {
		Constructor<T> cons = c.getConstructor(Traversal.class);
		return cons.newInstance(this);
	}

	public Object getCurrres() {
		return currres;
	}

	public Traversal filterBy(String... k) {
		addStep(new VarFilterStep(k, this, true));
		return this;
	}

	public Traversal save(String k) {
		addStep(new SaveStep(k, this));
		return this;
	}

	public Traversal traverse(int max_edges, Dir... dirs) {
		return this.traverse(new String[] {}, max_edges, dirs);
	}

	public Traversal traverse(String[] filters, int max_edges, Dir... dirs) {
		if (dirs.length == 0)
			return this;

		for (int i = 0; i < dirs.length; i++) {
			to(dirs[i], max_edges);
			filterBy(filters);
		}
		return this;
	}

	public Traversal traverseCount(Dir... dirs) {
		return traverseCount(new String[] {}, Integer.MAX_VALUE, dirs);
	}

	public Traversal traverseCount(String[] filters, int max_edges, Dir... dirs) {
		for (int i = 0; i < dirs.length; i++) {
			count(dirs[i], max_edges, filters);
		}
		return this;
	}

	public Traversal randomOut() {
		return random(Dir.OUT, new long[] {});
	}

	public Traversal random(Dir dir, long[] subset) {
		addStep(new RandomStep(dir, subset, this));
		return this;
	}

	public <T> Traversal each(int steps, String key, ForEach<T> forEach) {
		addStep(new EachStep(key, steps, forEach, this));
		return this;
	}

	public Traversal join(String from, String to, Join join) {
		addStep(new JoinStep(from, to, join, this));
		return this;
	}

	public Traversal filterNeg(long[] before) {
		addStep(new FilterStep(before, this, true));
		return this;
	}

	public Traversal repeat(int steps, Repeat<long[]> rfunc, RepeatSync<long[]> sync) {
		addStep(new RepeatStep(steps, rfunc, sync, this));
		return this;
	}

	public Traversal traverse(String[] strings, Dir[] copyOfRange) {
		return traverse(strings, Integer.MAX_VALUE, copyOfRange);
	}

	public Traversal to(Dir dir) {
		return to(dir, -1);
	}

	public Traversal filter(VertexFilter customFilter) {
		addStep(new CustomFilterStep(customFilter, this));
		return this;
	}

	public Traversal customStep(CustomFunction f) {
		addStep(new CustomStep(this, f));
		return this;
	}

	public Traversal top(int top) {
		addStep(new TopStep(top, this));
		return this;

	}

	public Traversal add(long... users) {
		addStep(new AddVertexStep(this, users));
		return this;
	}

	public Traversal intersect(Dir dir) {
		addStep(new IntersectStep(dir, this));
		return this;
	}

	public Traversal size() {
		addStep(new SizeStep());
		return this;
	}

	public Traversal expand(Dir dir, int max_edges) {
		addStep(new VertexStep(dir, max_edges, true, this));
		return this;
	}

	public Traversal traverseGraphCount(String countk, String kBeta, TLongHashSet vertices, int max_edges,
			Beta calc, Dir... dirs) {
		for (int i = 0; i < dirs.length; i++) {
			graphcount(calc, countk, vertices, dirs[i], max_edges, i != dirs.length - 1, kBeta);
		}
		return this;
	}

	public Traversal graphcount(Beta calc, String countk, TLongHashSet vertices, Dir dir, int max_edges,
			boolean returnVertices, String kBeta) {
		addStep(new GraphCountStep(calc, dir, vertices, max_edges, this, countk, returnVertices, kBeta));
		return this;
	}

	public Traversal mark(String string) {
		addStep(new MarkStep(string, this));
		return this;
	}
}
