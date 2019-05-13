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
package net.sf.jasperreports.engine.fill;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.collections4.map.ReferenceMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import net.sf.jasperreports.engine.JRRuntimeException;
import net.sf.jasperreports.engine.JRVirtualizable;
import net.sf.jasperreports.engine.util.SwapFileVirtualizerStore;


/**
 * @author Lucian Chirita (lucianc@users.sourceforge.net)
 */
public class StoreFactoryVirtualizer extends JRAbstractLRUVirtualizer
{
	private static final Log log = LogFactory.getLog(StoreFactoryVirtualizer.class);
	public static final String EXCEPTION_MESSAGE_KEY_STORE_NOT_FOUND = "fill.virtualizer.store.not.found";
	
	private final VirtualizerStoreFactory storeFactory;
	private final ReentrantLock contextStoresLock = new ReentrantLock();
	private final Map<JRVirtualizationContext, VirtualizerStore> contextStores;

	public StoreFactoryVirtualizer(int maxSize, VirtualizerStoreFactory storeFactory)
	{
		super(maxSize);

		this.storeFactory = storeFactory;
		
		this.contextStores = 
			Collections.synchronizedMap(new ReferenceMap<JRVirtualizationContext, VirtualizerStore>(
				ReferenceMap.ReferenceStrength.WEAK, ReferenceMap.ReferenceStrength.HARD
				));
	}

	protected VirtualizerStore store(JRVirtualizable o, boolean create)
	{
		JRVirtualizationContext masterContext = o.getContext().getMasterContext();
		return store(masterContext, create);
	}

	private boolean isDisposedForRemoval(VirtualizerStore store) {
		if(store == null) return true;
		if(store instanceof SwapFileVirtualizerStore) {
			SwapFileVirtualizerStore swapStore = (SwapFileVirtualizerStore)store;
			return swapStore.isSwapFileDisposed();
		}
		return false;
	}
	
	protected VirtualizerStore store(JRVirtualizationContext context, boolean create)
	{
		contextStoresLock.lock(); // need a lock around get/create/put of map
		try 
		{
			VirtualizerStore store = contextStores.get(context);
			
			// bit of a hack here to not return a disposed store, just return null so a new one can be created
			if(store != null && isDisposedForRemoval(store)) {
				if(log.isDebugEnabled())
				{
					log.debug("got disposed swap file store, will create a new one: " + store);
				}
				store = null;
			}

			if (store != null || !create)
			{
				if (log.isTraceEnabled())
				{
					log.trace("found " + store + " for " + context);
				}
				
				return store;
			}
			
			//the context should be locked at this moment
			store = storeFactory.createStore(context);
			if (log.isDebugEnabled())
			{
				log.debug("created " + store + " for " + context);
			}

			// TODO lucianc 
			// do we need to keep a weak reference to the context, and dispose the store when the reference is cleared?
			// not doing that for now, assuming that store objects are disposed when garbage collected.
			contextStores.put(context, store);

			return store;
		}
		finally
		{
			contextStoresLock.unlock();
		}
	}
	
	@Override
	protected void pageOut(JRVirtualizable o) throws IOException
	{
		o.getContext().lock();
		try 
		{
			VirtualizerStore store = store(o, true);
			boolean stored = store.store(o, serializer);
			if (!stored && !isReadOnly(o))
			{
				throw new IllegalStateException("Cannot virtualize data because the data for object UID \"" + o.getUID() + "\" already exists.");
			}
		}
		finally 
		{
			o.getContext().unlock();
		}
	}

	@Override
	protected void pageIn(JRVirtualizable o) throws IOException
	{
		o.getContext().lock();
		try 
		{
			VirtualizerStore store = store(o, false);
			if (store == null)
			{
				throw 
					new JRRuntimeException(
						EXCEPTION_MESSAGE_KEY_STORE_NOT_FOUND,
						new Object[]{o.getUID()});
			}
			
			store.retrieve(o, !isReadOnly(o), serializer);
		}
		finally 
		{
			o.getContext().unlock();
		}
	}

	@Override
	protected void dispose(JRVirtualizable o)
	{
		o.getContext().lock();
		try 
		{
			VirtualizerStore store = store(o, false);
			if (store == null)
			{
				if (log.isTraceEnabled())
				{
					log.trace("no store found for " + o.getUID() + " for disposal");
				}
				// not failing
				return;
			}
	
			store.remove(o.getUID());
		}
		finally 
		{
			o.getContext().unlock();
		}
	}
	
	@Override
	protected void dispose(String id)
	{
		// should not get here
		throw new UnsupportedOperationException();
	}

	public void dispose(JRVirtualizationContext context)
	{
		context.lock();
		try
		{
			// mark as disposed
			context.dispose();

			VirtualizerStore store = store(context, false);
			if (log.isDebugEnabled())
			{
				log.debug("found " + store + " for " + context + " for disposal");
			}
			
			if (store != null)
			{
				boolean usedByOtherContexts = false;
				for(Map.Entry<JRVirtualizationContext, VirtualizerStore> e : contextStores.entrySet()) 
				{
					if(!e.getKey().equals(context) && e.getValue().equals(store)) 
					{
						if (log.isDebugEnabled())
						{
							log.debug("found " + store + " used by other context " + context + " (will not dispose)");
						}
						usedByOtherContexts = true;
					}
				}
				
				if(!usedByOtherContexts) {
					
					store.dispose();
					// another check here, if the swap is closed, it probably is best to remove it from the map of stores
					if(store != null && isDisposedForRemoval(store)) {
						contextStores.remove(context);
					}
				}
			}
		}
		finally
		{
			context.unlock();
		}
	}
	
	@Override
	public void cleanup()
	{
		if (log.isDebugEnabled())
		{
			log.debug("disposing " + this);
		}

		contextStoresLock.lock();
		try
		{
			for (Iterator<?> it = contextStores.values().iterator(); it.hasNext();)
			{
				VirtualizerStore store = (VirtualizerStore) it.next();
				store.dispose();
			}

			contextStores.clear();
		}
		finally 
		{
			contextStoresLock.unlock();
		}
	}
}
