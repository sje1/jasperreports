/*
 * JasperReports - Free Java Reporting Library.
 * Copyright (C) 2001 - 2019 TIBCO Software Inc. All rights reserved.
 * http://www.jaspersoft.com
 *
 * Unless you have purchased a commercial license agreement from Jaspersoft,
 * the following license terms apply:
 *
 * This program is part of JasperReports.
 *
 * JasperReports is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * JasperReports is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with JasperReports. If not, see <http://www.gnu.org/licenses/>.
 */
package net.sf.jasperreports.engine.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import net.sf.jasperreports.engine.JRRuntimeException;
import net.sf.jasperreports.engine.JRVirtualizable;
import net.sf.jasperreports.engine.fill.VirtualizerStore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author Lucian Chirita (lucianc@users.sourceforge.net)
 */
public class SwapFileVirtualizerStore implements VirtualizerStore
{
	private static final Log log = LogFactory.getLog(SwapFileVirtualizerStore.class);
	public static final String EXCEPTION_MESSAGE_KEY_DEVIRTUALIZING_ERROR = "util.swap.file.virtualizer.devirtualizing.error";
	public static final String EXCEPTION_MESSAGE_KEY_UNABLE_TO_READ_DATA = "util.swap.file.virtualizer.unable.to.read.data";
	public static final String EXCEPTION_MESSAGE_KEY_VIRTUALIZING_ERROR = "util.swap.file.virtualizer.virtualizing.error";
	
	private final JRSwapFile swap;
	private final boolean swapOwner;
	private final Map<String,JRSwapFile.SwapHandle> handles;
	private final StreamCompression compression;
	
	
	/**
	 * A flag for the dispose behavior
	 * By default (the current behavior), dispose will remove the swap and file even with handles left unread/unremoved.
	 * Setting this flag to true will leave the file if there are still handles.
	 * This can help leave the store if one thread disposes a store, but another thread might still
	 * request a pageIn of a handle it has in the store later.
	 * A pageIn that cannot find a handle is a fatal error causing a report generation error.
	 */
	private boolean disposeOnlyWithEmptyHandles = false;

	public SwapFileVirtualizerStore(JRSwapFile swap, boolean swapOwner)
	{
		this(swap, swapOwner, null);
	}

	public SwapFileVirtualizerStore(JRSwapFile swap, boolean swapOwner, StreamCompression compression)
	{
		this.swap = swap;
		this.swapOwner = swapOwner;
		this.handles = Collections.synchronizedMap(new HashMap<String,JRSwapFile.SwapHandle>());
		this.compression = compression;
	}
	
	@Override
	public String toString()
	{
		return "SwapFileVirtualizerStore " + swap.toString(); 
	}
	
	protected boolean isStored(JRVirtualizable<?> o)
	{
		return handles.containsKey(o.getUID());
	}
	
	public void setDisposeOnlyWithEmptyHandles(boolean b) 
	{
		this.disposeOnlyWithEmptyHandles = b;
	}

	public boolean isDisposeOnlyWithEmptyHandles() 
	{
		return disposeOnlyWithEmptyHandles;
	}

	private static final ReentrantLock serializeLock = new ReentrantLock();
	
	
	@Override
	public boolean store(JRVirtualizable<?> o, VirtualizationSerializer serializer)
	{
		if (isStored(o))
		{
			if (log.isTraceEnabled())
			{
				log.trace("object " + o.getUID() + " already stored");
			}
			if(statsEnabled)
			{
				storeStats.isstored++;
				globalStats.isstored++;
			}
			return false;
		}
		
		long start = statsEnabled ? System.nanoTime() : 0;
		ReusableByteArrayOutputStream bout = ReusableByteArrayOutputStream.get();
		OutputStream out = compression == null ? bout : compression.compressedOutput(bout);
		try {
			serializeLock.lock(); // need to lock here for thread safety
			try {
				serializer.writeData(o, out);
			} finally {
				serializeLock.unlock();
			}
			out.close();
			long end = statsEnabled ? recordSerializeWrite(start) : 0; 

			start = statsEnabled ? System.nanoTime() : 0;
			byte[] data = bout.myInternalBuffer();
			int length = bout.size();
			JRSwapFile.SwapHandle handle = swap.write(data, length);
			end = statsEnabled ? recordWrite(start, length) : 0;
			handles.put(o.getUID(), handle);
			return true;
		}
		catch (IOException e)
		{
				log.error("Error virtualizing object " + o.getUID() + " to " + swap, e);
				throw 
					new JRRuntimeException(
						EXCEPTION_MESSAGE_KEY_VIRTUALIZING_ERROR,
						(Object[])null,
						e);
		} 
		finally 
		{
			if(out != null) 
			{
				try 
				{
					out.close();
				} catch(Exception ex) 
				{
					
				}
			}
		}
	}
	
	@Override
	public void retrieve(JRVirtualizable<?> o, boolean remove, VirtualizationSerializer serializer)
	{
		JRSwapFile.SwapHandle handle = handles.get(o.getUID());
		if (handle == null)
		{
			// should not happen
			//FIXME lucianc happened once, look into it
			log.error("No swap handle found for " + o.getUID() + " in " + this);
			throw 
				new JRRuntimeException(
					EXCEPTION_MESSAGE_KEY_UNABLE_TO_READ_DATA,
					(Object[])null);
		}
		
		JRSwapFile.ReadResult data = null;
		try
		{
			long start = statsEnabled ? System.nanoTime() : 0;
			data = swap.read(handle, remove);
			if (log.isTraceEnabled())
			{
				log.trace("read " + data.totalLength + " for object " + o.getUID() + " from " + swap);
			}
			long end = statsEnabled ? recordSwapRead(start) : 0; 
			
			start = statsEnabled ? System.nanoTime() : 0;
			ByteArrayInputStream rawInput = new ByteArrayInputStream(data.data, 0, data.totalLength);
			try 
			{
				InputStream input = compression == null ? rawInput : compression.uncompressedInput(rawInput);
				try {
					serializer.readData(o, input);
				} finally {
					if(compression != null) 
					{
						try 
						{
							input.close();
						} 
						catch(Exception ex) {
							
						}
					}
				}
			} finally 
			{
				try 
				{
					rawInput.close();
				} 
				catch(Exception ex) 
				{
					
				}
			}
			end = statsEnabled ? recordRead(start, data.data.length) : 0; 
		}
		catch (IOException e)
		{
			log.error("Error reading object data " + o.getUID() + " from " + swap, e);
			throw 
				new JRRuntimeException(
					EXCEPTION_MESSAGE_KEY_DEVIRTUALIZING_ERROR,
					(Object[])null,
					e);
		}
		
		if (remove)
		{
			handles.remove(o.getUID());
		}
	}
	
	@Override
	public void remove(String objectId)
	{
		JRSwapFile.SwapHandle handle = handles.remove(objectId);
		if (handle == null)
		{
			if (log.isTraceEnabled())
			{
				log.trace("object " + objectId + " not found for removal");
			}
		}
		else
		{
			if (log.isTraceEnabled())
			{
				log.trace("removing object " + objectId + " from " + swap);
			}
			
			swap.free(handle);
		}
	}

	private boolean disposed = false;
	
	/**
	 * If the store has been disposed (dispose has been called at least once).
	 * This does not mean the swap file underlying has been disposed, only if it is the owner if the swap file
	 * @return boolean If dispose has been called on this file store.
	 */
	public boolean isDisposed() {
		return disposed;
	}

	/**
	 * If the underlying swap file has been disposed.
	 * This does not mean the swap file underlying has been disposed, only if it is the owner if the swap file
	 * This will be true if dispose has been called, and this swap store is the owner of the swap file. 
	 * @return boolean If the underlying swap file has been disposed.
	 */
	public boolean isSwapFileDisposed() {
		return swap.isDisposed();
	}
	
	/**
	 * If this swap has uids (handles).
	 * A store call will register a handle (uid), and a retrieve with remove, or a remove will remove the uid handle.
	 * @return boolean if there any handles currently in this swap store.
	 */
	public boolean hasHandles() {
		return handles.isEmpty();
	}

	/**
	 * Disposes the swap file used if this virtualizer owns it.
	 * @see #SwapFileVirtualizerStore(JRSwapFile, boolean)
	 */
	@Override
	public void dispose()
	{
		// new optional check...ignore dispose if there are handles present
		if(disposeOnlyWithEmptyHandles) 
		{
			if(!handles.isEmpty()) 
			{
				return;
			}
		}
		
		handles.clear();
		if (swapOwner)
		{
			if (log.isDebugEnabled())
			{
				log.debug("disposing " + swap);
			}
			
			swap.dispose();
		}
		disposed = true;
	}

	/**
	 * Can be set to enable keep basic stats (counters) of bytes, reads, writes, and durations for the store
	 * This could be used by tests or runtime monitoring of a jasper system, such as via JMX
	 * The overhead of keeping these counts intends to be minimal and ok for production. 
	 */
	private boolean statsEnabled = false;
	
	/**
	 * Stats for this store
	 */
	private SwapFileVirtualizerStoreStats storeStats = new SwapFileVirtualizerStoreStats();
	
	/**
	 * Global stats for the the lifetime of the JVM across all stores 
	 */
	private static final SwapFileVirtualizerStoreStats globalStats = new SwapFileVirtualizerStoreStats();
	
	private long recordRead(long startNanos, int dataLength) 
	{
		long endNanos = System.nanoTime();
		storeStats.reads++;
		globalStats.reads++;
		storeStats.readBytes += dataLength;
		globalStats.readBytes += dataLength;
		storeStats.readSerializerNanos += (endNanos-startNanos);
		globalStats.readSerializerNanos += (endNanos-startNanos);
		return endNanos;
	}
	
	private long recordSwapRead(long startNanos) 
	{
		long end = System.nanoTime();
		storeStats.readSwapNanos += (end-startNanos);
		globalStats.readSwapNanos += (end-startNanos);
		return end;
	}
	
	private long recordWrite(long startNanos, int length) 
	{
		long end = System.nanoTime();
		storeStats.stored++;
		globalStats.stored++;
		storeStats.storedBytes += length;
		globalStats.storedBytes += length;
		storeStats.storeSwapNanos += (end-startNanos);
		globalStats.storeSwapNanos += (end-startNanos);
		return end;
	}
	
	private long recordSerializeWrite(long startNanos) 
	{
		long end = System.nanoTime();
		storeStats.storeSerializerNanos += (end-startNanos);
		globalStats.storeSerializerNanos += (end-startNanos);
		return end;
	}
	
	void setStatsEnabled(boolean statsEnabled) 
	{
		this.statsEnabled = statsEnabled;
	}
	
	/**
	 * Keep some stats 
	 */
	public static class SwapFileVirtualizerStoreStats 
	{
		private int stored = 0;
		private int isstored = 0;
		private long storedBytes = 0;
		private long storeSerializerNanos = 0;
		private long storeSwapNanos = 0;
		private int reads = 0;
		private long readBytes = 0;
		private long readSerializerNanos = 0;
		private long readSwapNanos = 0;
	
		public void resetStats() {
			stored = 0;
			isstored = 0;
			storedBytes = 0;
			storeSerializerNanos = 0;
			storeSwapNanos = 0;
			reads = 0;
			readBytes = 0;
			readSerializerNanos = 0;
			readSwapNanos = 0;
		}
		
		public double storeHitRatio() {
			return isstored / stored;
		}
		public long storedBytes() {
			return storedBytes;
		}
		public double averageStoredBytes() {
			return storedBytes / stored;
		}
		public long readBytes() {
			return readBytes;
		}
		public double averageReadBytes() {
			return readBytes / reads;
		}
		public long serializeNanos() { 
			return storeSerializerNanos;
		}
		public long deserializeNanos() { 
			return readSerializerNanos;
		}
		public long writeNanos() { 
			return storeSwapNanos;
		}
		public long readNanos() { 
			return readSwapNanos;
		}
	}
	
	/**
	 * The stats for this store. One store maps to one swap file, so the stats are just for the one swap file.
	 * @return SwapFileVirtualizerStoreStats the stats object
	 */
	public SwapFileVirtualizerStoreStats getStoreStats() 
	{
		return storeStats;
	}
	
	/**
	 * The stats for the instance (aggregates across all stores for the lifetime of the JVM)
	 * @return SwapFileVirtualizerStoreStats the stats object
	 */
	public SwapFileVirtualizerStoreStats getGlobalStats() 
	{
		return globalStats;
	}
}
