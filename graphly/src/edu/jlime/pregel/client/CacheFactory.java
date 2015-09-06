package edu.jlime.pregel.client;

import java.io.Serializable;

import edu.jlime.pregel.worker.CacheManagerI;
import edu.jlime.pregel.worker.CacheManagerImpl;
import edu.jlime.pregel.worker.NoCache;
import edu.jlime.pregel.worker.WorkerTask;

public interface CacheFactory extends Serializable {
	CacheFactory NO_CACHE = new CacheFactory() {

		@Override
		public CacheManagerI build(WorkerTask task, PregelConfig config) {
			return new NoCache(task);
		}
	};

	CacheFactory SIMPLE = new CacheFactory() {

		@Override
		public CacheManagerI build(WorkerTask task, PregelConfig config) {
			return new CacheManagerImpl(task, config);
		}
	};

	CacheManagerI build(WorkerTask task, PregelConfig config);
}
