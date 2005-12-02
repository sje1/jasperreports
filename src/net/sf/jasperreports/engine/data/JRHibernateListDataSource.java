/*
 * ============================================================================
 * GNU Lesser General Public License
 * ============================================================================
 *
 * JasperReports - Free Java report-generating library.
 * Copyright (C) 2001-2005 JasperSoft Corporation http://www.jaspersoft.com
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307, USA.
 * 
 * JasperSoft Corporation
 * 303 Second Street, Suite 450 North
 * San Francisco, CA 94107
 * http://www.jaspersoft.com
 */
package net.sf.jasperreports.engine.data;

import java.util.Iterator;
import java.util.List;

import net.sf.jasperreports.engine.JRRewindableDataSource;
import net.sf.jasperreports.engine.query.JRHibernateQueryExecuter;

/**
 * Hibernate data source that uses <code>org.hibernate.Query.list()</code>.
 * <p/>
 * The query result can be paginated by not retrieving all the rows at once.
 * 
 * @author Lucian Chirita (lucianc@users.sourceforge.net)
 * @version $Id$
 * @see net.sf.jasperreports.engine.query.JRHibernateQueryExecuterFactory#PROPERTY_HIBERNATE_QUERY_LIST_PAGE_SIZE
 */
public class JRHibernateListDataSource extends JRHibernateAbstractDataSource implements JRRewindableDataSource
{
	private final int pageSize;
	private int pageCount;
	private boolean nextPage;
	private List returnValues;
	private Iterator iterator;

	public JRHibernateListDataSource(JRHibernateQueryExecuter queryExecuter, boolean useFieldDescription, int pageSize)
	{
		super(queryExecuter, useFieldDescription, false);

		this.pageSize = pageSize;
		
		pageCount = 0;
		fetchPage();
	}

	protected void fetchPage()
	{
		if (pageSize <= 0)
		{
			returnValues = queryExecuter.list();
			nextPage = false;
		}
		else
		{
			returnValues = queryExecuter.list(pageCount * pageSize, pageSize);
			nextPage = returnValues.size() == pageSize;
		}

		++pageCount;

		initIterator();
	}

	public boolean next()
	{
		if (iterator == null)
		{
			return false;
		}
		
		boolean hasNext = iterator.hasNext();
		if (!hasNext && nextPage)
		{
			fetchPage();
			hasNext = iterator != null && iterator.hasNext();
		}
		
		if (hasNext)
		{
			setCurrentRowValue(iterator.next());
		}

		return hasNext;
	}

	public void moveFirst()
	{
		if (pageCount == 1)
		{
			initIterator();
		}
		else
		{
			pageCount = 0;
			fetchPage();
		}
	}

	private void initIterator()
	{
		iterator = returnValues == null ? null : returnValues.iterator();
	}
}
