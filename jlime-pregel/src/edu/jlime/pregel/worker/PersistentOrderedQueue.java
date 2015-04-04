package edu.jlime.pregel.worker;

import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongObjectHashMap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.mapdb.BTreeMap;
import org.mapdb.DBMaker;

public class PersistentOrderedQueue {
	AtomicLong messageID = new AtomicLong(0);

	public static class MessageID implements Serializable,
			Comparable<MessageID> {
		long vid;
		long messageid;

		public MessageID(long id, long mid) {
			this.vid = id;
			this.messageid = mid;
		}

		@Override
		public int compareTo(MessageID o) {
			int comp = Long.compare(vid, o.vid);
			if (comp == 0)
				return Long.compare(messageid, o.messageid);
			return comp;
		}
	}

	AtomicInteger readcont = new AtomicInteger(0);
	Semaphore rlock = new Semaphore(1);
	Semaphore wlock = new Semaphore(1);

	AtomicInteger cont = new AtomicInteger(0);
	AtomicInteger contBacked = new AtomicInteger(0);
	private volatile TLongObjectHashMap<List<PregelMessage>> cache = new TLongObjectHashMap<>();
	private volatile BTreeMap<MessageID, PregelMessage> back;
	private int max;
	private int initSize;
	private MessageMerger merger;

	public PersistentOrderedQueue(MessageMerger merger, int max,
			int initialListSize) {
		this.merger = merger;
		this.max = max;
		this.initSize = initialListSize;
		this.back = DBMaker.newTempFileDB().transactionDisable()
				.asyncWriteEnable().cacheDisable().make().createTreeMap("")
				.<MessageID, PregelMessage> make();

	}

	public void put(PregelMessage pregelMessage) {
		int i = cont.incrementAndGet();
		if (i > max) {
			contBacked.incrementAndGet();
			back.put(
					new MessageID(pregelMessage.to, messageID.getAndIncrement()),
					pregelMessage);
		} else {
			long to = pregelMessage.getTo();

			List<PregelMessage> list = null;
			synchronized (cache) {
				list = cache.get(to);
				if (list == null) {
					list = new ArrayList<PregelMessage>(initSize);
					cache.put(to, list);
				}
			}
			// try {
			// rlock.acquire();
			// synchronized (rlock) {
			// int currRead = readcont.getAndIncrement();
			// if (currRead == 0) {
			// wlock.acquire();
			// }
			// }
			// rlock.release();
			//
			// list = cache.get(to);
			// if (list == null) {
			//
			// synchronized (rlock) {
			// int currRead = readcont.getAndDecrement();
			// if (currRead == 1) {
			// wlock.release();
			// }
			// }
			//
			// rlock.acquire();
			// wlock.acquire();
			// list = cache.get(to);
			// if (list == null) {
			// list = new ArrayList<PregelMessage>(initSize);
			// cache.put(to, list);
			// }
			// wlock.release();
			// rlock.release();
			// } else {
			// synchronized (rlock) {
			// int currRead = readcont.getAndDecrement();
			// if (currRead == 1) {
			// wlock.release();
			// }
			// }
			// }
			// } catch (InterruptedException e) {
			// // TODO Auto-generated catch block
			// e.printStackTrace();
			// }

			synchronized (list) {
				if (list.size() >= 2)
					System.out.println("Not right");

				if (merger == null || list.isEmpty()) {
					pregelMessage.setFrom(-1);
					list.add(pregelMessage);
				} else {
					list.get(0).setV(
							merger.merge(list.get(0).getV(),
									pregelMessage.getV()));
				}
			}
		}
	}

	public void clear() {
		TLongObjectIterator<List<PregelMessage>> it = cache.iterator();
		while (it.hasNext()) {
			it.advance();
			it.value().clear();
		}
		if (contBacked.get() > 0)
			back.clear();
		cont.set(0);
		messageID.set(0);
		contBacked.set(0);
	}

	public Iterator<List<PregelMessage>> iterator() {
		Iterator<Entry<MessageID, PregelMessage>> backIt = null;
		if (contBacked.get() > 0)
			backIt = back.entrySet().iterator();
		TLongArrayList list = new TLongArrayList(cache.keys());
		list.sort();
		return new PersistentQueueIterator(backIt, list, cache);
	}

	public int size() {
		return cont.get();
	}
}
