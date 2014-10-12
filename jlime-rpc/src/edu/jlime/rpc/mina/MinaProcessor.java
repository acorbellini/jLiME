package edu.jlime.rpc.mina;

import java.net.InetSocketAddress;

import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;

import edu.jlime.core.transport.DataReceiver;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.rpc.message.Message;
import edu.jlime.rpc.message.MessageProcessor;
import edu.jlime.rpc.message.SimpleMessageProcessor;

public class MinaProcessor extends SimpleMessageProcessor implements
		DataReceiver {

	private class MessageHandler extends IoHandlerAdapter {
		public MessageHandler(MinaProcessor minaProcessor) {
		}

		@Override
		public void messageReceived(IoSession session, Object message)
				throws Exception {
			super.messageReceived(session, message);
		}
	}

	private NioSocketAcceptor acceptor;
	private int port;

	public MinaProcessor(MessageProcessor next, String name, int port) {
		super(next, name);
		this.port = port;
	}

	@Override
	public void dataReceived(byte[] array, InetSocketAddress addr)
			throws Exception {

	}

	@Override
	public void onStart() throws Exception {
		acceptor = new NioSocketAcceptor();

		// acceptor.getFilterChain().addLast( "logger", new LoggingFilter() );
		// acceptor.getFilterChain().addLast( "codec", new ProtocolCodecFilter(
		// new TextLineCodecFactory( Charset.forName( "UTF-8" ))));

		acceptor.setHandler(new MessageHandler(this));
		acceptor.getSessionConfig().setReadBufferSize(2048);
		acceptor.getSessionConfig().setIdleTime(IdleStatus.BOTH_IDLE, 10);
		acceptor.bind(new InetSocketAddress(port));
	}

	@Override
	public void setMetrics(Metrics metrics) {
	}

	@Override
	protected void send(Message msg) throws Exception {
		// TODO Auto-generated method stub

	}
}
