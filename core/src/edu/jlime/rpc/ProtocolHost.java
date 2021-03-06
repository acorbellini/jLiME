/*
 * Copyright (c) 2014 mucaho (https://github.com/mucaho).
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package edu.jlime.rpc;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.esotericsoftware.kryo.io.KryoObjectInput;
import com.esotericsoftware.kryo.io.KryoObjectOutput;
import com.github.mucaho.jnetrobust.Logger;
import com.github.mucaho.jnetrobust.Protocol;
import com.github.mucaho.jnetrobust.ProtocolListener;
import com.github.mucaho.jnetrobust.control.Metadata;
import com.github.mucaho.jnetrobust.controller.Packet;

import edu.jlime.core.transport.Address;

public class ProtocolHost<T> {
	public static interface DataListener<T> {
		public void handleOrderedData(T orderedData);

		public void handleNewestData(T newestData);
		// TODO add exceptional callback
	}

	public static class ProtocolHandle<T> {
		private final ProtocolId protocolId;
		private final ProtocolHost<T> host;

		private ProtocolHandle(ProtocolId protocolId, ProtocolHost<T> host) {
			this.host = host;
			this.protocolId = protocolId;
		}

		public void send(T data) throws IOException {
			host.send(protocolId, data);
		}

		public T receive() throws IOException, ClassNotFoundException {
			host.receive();
			return host.receive(protocolId);
		}

		public ProtocolId getProtocolId() {
			return protocolId;
		}
	}

	private final String hostName;

	// protocol fields
	private final Map<ProtocolId, Protocol<T>> protocols = new ConcurrentHashMap<ProtocolId, Protocol<T>>();
	private final Map<ProtocolId, DataListener<T>> listeners = new ConcurrentHashMap<ProtocolId, DataListener<T>>();

	// serialization fields
	private final Kryo kryo;
	private final ByteBuffer receiveBuffer = ByteBuffer.allocate(2048);
	private final ByteBuffer writebuffer = ByteBuffer.allocate(2048);
	private final ByteBufferInput bufferInput = new ByteBufferInput();
	private final ByteBufferOutput bufferOutput = new ByteBufferOutput();
	private final KryoObjectInput objectInput;
	private final KryoObjectOutput objectOutput;

	// network communication fields
	private final DatagramChannel channel;

	public ProtocolHost(String hostName, Class<T> dataClass, SocketAddress localAddress) throws IOException {
		// setup network communication
		channel = DatagramChannel.open();
		channel.configureBlocking(false);
		channel.bind(localAddress);

		// setup serialization
		kryo = new Kryo();
		kryo.register(Packet.class); // add argument `new
										// ExternalizableSerializer()` if needed
		kryo.register(Metadata.class); // add argument `new
										// ExternalizableSerializer()` if needed
		kryo.register(dataClass);
		objectInput = new KryoObjectInput(kryo, bufferInput);
		objectOutput = new KryoObjectOutput(kryo, bufferOutput);

		this.hostName = hostName;
	}

	public ProtocolHandle<T> register(Address topic, SocketAddress remoteAddress) {
		return register(topic, remoteAddress, null);
	}

	public ProtocolHandle<T> register(Address topic, SocketAddress remoteAddress, final DataListener<T> listener) {
		ProtocolId protocolId = new ProtocolId(topic, remoteAddress);

		ProtocolListener<T> protocolListener = new ProtocolListener<T>() {
			@Override
			public void handleOrderedData(short dataId, T orderedData) {
				if (listener != null)
					listener.handleOrderedData(orderedData);
			}
		};
		if (listener != null)
			listeners.put(protocolId, listener);

		Protocol<T> protocol;
		if (hostName != null)
			protocol = new Protocol<T>(protocolListener, Logger.getConsoleLogger(hostName));
		else
			protocol = new Protocol<T>(protocolListener);
		protocols.put(protocolId, protocol);

		return new ProtocolHandle<T>(protocolId, this);
	}

	private synchronized void send(ProtocolId protocolId, T data) throws IOException {
		writebuffer.clear();
		writebuffer.putLong(protocolId.topic.getId().getMostSignificantBits());
		writebuffer.putLong(protocolId.topic.getId().getLeastSignificantBits());

		bufferOutput.setBuffer(writebuffer);

		protocols.get(protocolId).send(data, objectOutput);

		writebuffer.flip();
		synchronized (channel) {
			int write = 0;
			while ((write += channel.send(writebuffer, protocolId.remoteAddress)) != writebuffer.limit()) {
			}
		}
	}

	private Map<ProtocolId, Short> newestIds = new ConcurrentHashMap<ProtocolId, Short>();
	private Map<ProtocolId, T> newestDatas = new ConcurrentHashMap<ProtocolId, T>();
	private Map<ProtocolId, Queue<T>> receivedQueues = new ConcurrentHashMap<ProtocolId, Queue<T>>();

	private List<HandleListener<T>> handleListeners = new ArrayList<>();

	private synchronized void receive() throws IOException, ClassNotFoundException {
		receiveBuffer.clear();
		SocketAddress remoteAddress = channel.receive(receiveBuffer);
		while (remoteAddress != null) {
			receiveBuffer.flip();
			ProtocolId protocolId = new ProtocolId(
					new Address(new UUID(receiveBuffer.getLong(), receiveBuffer.getLong())), remoteAddress);
			bufferInput.setBuffer(receiveBuffer);

			Protocol<T> protocol = protocols.get(protocolId);

			if (protocol == null) {
				ProtocolHandle<T> handle = register(protocolId.topic, protocolId.remoteAddress);
				protocol = protocols.get(protocolId);
				for (HandleListener<T> handleListener : handleListeners) {
					handleListener.added(handle);
				}

			}

			NavigableMap<Short, T> receivedEntries = protocol.receive(objectInput);
			for (Map.Entry<Short, T> receivedEntry : receivedEntries.entrySet()) {
				Short receivedId = receivedEntry.getKey();
				T receivedData = receivedEntry.getValue();

				{
					Queue<T> receivedQueue = receivedQueues.get(protocolId);
					if (receivedQueue == null) {
						synchronized (receivedQueues) {
							receivedQueue = receivedQueues.get(protocolId);
							if (receivedQueue == null) {
								receivedQueue = new LinkedList<T>();
								receivedQueues.put(protocolId, receivedQueue);
							}
						}
					}
					synchronized (receivedQueue) {
						receivedQueue.offer(receivedData);
					}
				}
				{
					Short newestId = newestIds.get(protocolId);
					if (newestId == null || protocol.compare(receivedId, newestId) > 0) {
						newestIds.put(protocolId, receivedId);
						newestDatas.put(protocolId, receivedData);
					}
				}
			}

			receiveBuffer.clear();
			remoteAddress = channel.receive(receiveBuffer);
		}
	}

	private synchronized T receive(ProtocolId protocolId) {
		Queue<T> receivedQueue = receivedQueues.get(protocolId);
		if (receivedQueue == null)
			return null;
		T receivedData;
		synchronized (receivedQueue) {
			receivedData = receivedQueue != null ? receivedQueue.poll() : null;
		}

		if (receivedData == null) {
			T newestData = newestDatas.remove(protocolId);
			if (newestData != null) {
				DataListener<T> listener = listeners.get(protocolId);
				if (listener != null)
					listener.handleNewestData(newestData);
			}
			receivedData = newestData;
		}

		return receivedData;
	}

	public static class ProtocolId {
		private final Address topic;
		private final SocketAddress remoteAddress;

		private ProtocolId(Address topic, SocketAddress remoteAddress) {
			this.topic = topic;
			this.remoteAddress = remoteAddress;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o)
				return true;
			if (o == null || getClass() != o.getClass())
				return false;

			ProtocolId that = (ProtocolId) o;

			if (!topic.equals(that.topic))
				return false;
			if (!remoteAddress.equals(that.remoteAddress))
				return false;
			return true;
		}

		@Override
		public int hashCode() {
			int result = (int) topic.hashCode();
			result = 31 * result + remoteAddress.hashCode();
			return result;
		}

		public Address getTopic() {
			return topic;
		}

		public SocketAddress getRemoteAddress() {
			return remoteAddress;
		}
	}

	public void addHandleListener(HandleListener<T> list) {
		handleListeners.add(list);
	}
}
