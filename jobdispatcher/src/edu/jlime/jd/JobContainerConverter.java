package edu.jlime.jd;

import java.util.UUID;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.marshalling.TypeConverter;
import edu.jlime.core.marshalling.TypeConverters;
import edu.jlime.util.ByteBuffer;

public class JobContainerConverter implements TypeConverter {
	private final TypeConverters tc;

	JobContainerConverter(TypeConverters tc) {
		this.tc = tc;
	}

	@Override
	public void toArray(Object o, ByteBuffer buffer, Peer cliID)
			throws Exception {
		JobContainer jc = (JobContainer) o;
		tc.objectToByteArray(jc.getRequestor(), buffer, cliID);
		tc.objectToByteArray(jc.getJob(), buffer, cliID);
		buffer.putUUID(jc.getJobID());
		buffer.putBoolean(jc.isNoresponse());
	}

	@Override
	public Object fromArray(ByteBuffer buff) throws Exception {

		ClientNode p = (ClientNode) tc.getObjectFromArray(buff);

		ClientJob<?> job = (ClientJob<?>) tc.getObjectFromArray(buff);
		UUID id = buff.getUUID();
		boolean isNoResponse = buff.getBoolean();
		JobContainer jc = new JobContainer(job, p);
		jc.setID(id);
		jc.setNoResponse(isNoResponse);
		return jc;
	}
}