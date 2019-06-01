package net.sf.jasperreports.engine.util;

import java.io.ByteArrayOutputStream;

/**
 * A 're-usable' ByteArrayOutputStream
 * So we just extend the ByteArrayOutputStream and get access to the internal byte[]
 * so we don't have to call toByteArray and do an full array copy everytime to want the value,
 * and we can reuse it
 */
public final class ReusableByteArrayOutputStream extends ByteArrayOutputStream {
	/**
	 * Thread local byte arrays for output, to act as a cache for re-use of byte arrays
	 */
	private static final ThreadLocal<ReusableByteArrayOutputStream> bouts = new ThreadLocal<>();

	private static final int startSize = 1024*100;
	
	public static ReusableByteArrayOutputStream get() {
		ReusableByteArrayOutputStream bout = bouts.get();
		if(bout == null) {
			bout = new ReusableByteArrayOutputStream(startSize);
			bouts.set(bout);
		}
		bout.reset();
		return bout;
	}
	private ReusableByteArrayOutputStream(int size) { 
		super(size); 
	}
	public byte[] myInternalBuffer() { 
		return buf; 
	}
}

