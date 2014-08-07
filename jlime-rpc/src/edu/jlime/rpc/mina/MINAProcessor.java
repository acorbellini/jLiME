package edu.jlime.rpc.mina;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.UUID;

import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.apache.mina.transport.socket.nio.NioSocketConnector;

import edu.jlime.core.stream.RemoteInputStream;
import edu.jlime.core.stream.RemoteOutputStream;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.rpc.SocketFactory;
import edu.jlime.rpc.message.AddressType;
import edu.jlime.rpc.message.Address;
import edu.jlime.rpc.message.SocketAddress;
import edu.jlime.rpc.np.NetworkProtocol;
import edu.jlime.rpc.np.DataPacket;

public class MINAProcessor extends NetworkProtocol {

	public MINAProcessor(String addr, int port, int range, SocketFactory fact,
			UUID id) {
		super(addr, port, range, fact, id);
	}

	@Override
	public List<SocketAddress> getAddresses() {
		return null;
	}

	@Override
	public RemoteInputStream getInputStream(UUID streamId, Address from) {
		return null;
	}

	@Override
	public RemoteOutputStream getOutputStream(UUID streamId, Address to) {
		return null;
	}

	@Override
	protected void beforeProcess(DataPacket pkt, Address from, Address to) {

	}

	@Override
	public AddressType getType() {
		return AddressType.MINA;
	}

	@Override
	public void onStart(Object sock) {
		IoAcceptor acceptor = new NioSocketAcceptor();
		acceptor.setHandler(new IoHandlerAdapter() {
			@Override
			public void messageReceived(IoSession session, Object message)
					throws Exception {

			}
		});
		acceptor.getSessionConfig().setReadBufferSize(2048);
		acceptor.getSessionConfig().setIdleTime(IdleStatus.BOTH_IDLE, 10);
		try {
			acceptor.bind(new InetSocketAddress(getPort()));
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void sendBytes(byte[] built, Address to, SocketAddress realSockAddr)
			throws Exception {
		NioSocketConnector connector = new NioSocketConnector();
		ConnectFuture future = connector.connect(new InetSocketAddress(
				realSockAddr.getSockTo().getHostName(), realSockAddr
						.getSockTo().getPort()));
		IoSession session = future.getSession();
		session.write(built);
	}

	@Override
	public void setMetrics(Metrics metrics) {

	}
}
