package net.sf.jasperreports.engine.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A 're-usable' ByteArray (byte[]) as a ThreadLocal
 * Note the byte[] can be any size, but just at least as long as requested.
 * So the caller must always know the data size, and not ever use the length of the byte[] as the length of the data.
 */
public final class ReusableByteArray {
	private static final Log log = LogFactory.getLog(ReusableByteArray.class);

	/**
	 * Thread local byte arrays for use in reads.
	 * This is used as a way to cache and reuse arrays.
	 */
	private static final ThreadLocal<byte[]> threadLovalArrays = new ThreadLocal<byte[]>() {};
	private static int startingBufferLength = 1024*10;
	
	/**
	 * Get a byte array for reading input data.
	 * The length provided indicates the array must be at least this length.
	 * This method uses the thread local to re-use arrays.
	 * @param length The requested minimum length of the buffer (note the buffer returned will be at least this long, but likely longer
	 * @return byte[] The buffer
	 */
	public static byte[] getByteArray(int length) {
		byte[] data = threadLovalArrays.get();
		if(data == null) 
		{
			int newLength = length < startingBufferLength ? startingBufferLength : length;
			if(log.isDebugEnabled()) log.debug("creating buffer: requestedLength=" + length + ", newLength=" + newLength);
			data = new byte[newLength];
			threadLovalArrays.set(data);
		} 
		else if(data.length < length) 
		{
			int newlength = ((length - data.length) / data.length) < 0.25d ? (int)(1.25d * data.length) : length;
			if(log.isDebugEnabled()) log.debug("resizing buffer: bufferLength=" + data.length + ", requestedLength=" + length + ", newLength=" + newlength);
			data = new byte[newlength];
			threadLovalArrays.set(data);
		}
		return data;
	}
}

