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
import java.io.ByteArrayOutputStream;
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
			return false;
		}
		
		try
		{
			ByteArrayOutputStream bout = new ByteArrayOutputStream(3000);
			OutputStream out = compression == null ? bout : compression.compressedOutput(bout);
			serializeLock.lock(); // need to lock here for thread safety
			try {
				serializer.writeData(o, out);
			} finally {
				serializeLock.unlock();
			}
			out.close();
			
			byte[] data = bout.toByteArray();
			if (log.isTraceEnabled())
			{
				log.trace("writing " + data.length + " for object " + o.getUID() + " to " + swap);
			}
			
			JRSwapFile.SwapHandle handle = swap.write(data);
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
		
		try
		{
			byte[] data = swap.read(handle, remove);
			if (log.isTraceEnabled())
			{
				log.trace("read " + data.length + " for object " + o.getUID() + " from " + swap);
			}
			
			ByteArrayInputStream rawInput = new ByteArrayInputStream(data);
			InputStream input = compression == null ? rawInput : compression.uncompressedInput(rawInput);
			serializer.readData(o, input);
			input.close();
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
}
