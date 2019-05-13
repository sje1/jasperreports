package net.sf.jasperreports.engine.fill;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.Assert;
import org.testng.annotations.Test;

import net.sf.jasperreports.AbstractTest;
import net.sf.jasperreports.engine.JasperPrint;
import net.sf.jasperreports.engine.SimpleJasperReportsContext;
import net.sf.jasperreports.engine.base.ElementsBlock;
import net.sf.jasperreports.engine.base.JRBasePrintText;
import net.sf.jasperreports.engine.base.JRVirtualPrintPage;
import net.sf.jasperreports.engine.util.SwapFileVirtualizerStoreFactory;

/**
 * To reproduce an issue in jasper reports server generating scheduled reports using the 'SharedVirtualizerStoreFactory'.
 * The 'SharedVirtualizerStoreFactory' seems to be the best option for busy, overlapping, large scheduled reports.
 * This should be considered a normal usage pattern of enterprise jasper, and it should be expected to handle this scenario.
 * Typically the workaround for us has been to keep spreading schedules farther apart to mitigate and prevent this issue,
 * however this only goes so far, and it is neccessary to support large overlapping report schedules.
 * (the 'FileVirtualizerFactory' does not seem to appropriate scale like the 'SharedVirtualizerStoreFactory').
 * 
 * With multiple overlapping large scheduled reports, intermittent report generation failures occur.
 * The error occurs in a pageOut request on a closed file (a 'disposed' JRSwapFile).
 * The error is 'Caused by: java.io.IOException: Stream Closed'
 * 
ERROR ReportExecutionJob:363 - The report was not completed. An error occurred while executing it.
net.sf.jasperreports.engine.JRRuntimeException: Error virtualizing object.
        at net.sf.jasperreports.engine.util.SwapFileVirtualizerStore.store(SwapFileVirtualizerStore.java:113)
        at net.sf.jasperreports.engine.fill.StoreFactoryVirtualizer.pageOut(StoreFactoryVirtualizer.java:98)
        at net.sf.jasperreports.engine.fill.JRAbstractLRUVirtualizer.virtualizeData(JRAbstractLRUVirtualizer.java:592)
        at net.sf.jasperreports.engine.fill.JRAbstractLRUVirtualizer.evict(JRAbstractLRUVirtualizer.java:407)
        at net.sf.jasperreports.engine.fill.JRAbstractLRUVirtualizer.registerObject(JRAbstractLRUVirtualizer.java:361)
        at net.sf.jasperreports.engine.base.ElementsBlock.register(VirtualizableElementList.java:253)
        at net.sf.jasperreports.engine.base.ElementsBlock.preAdd(VirtualizableElementList.java:304)
        at net.sf.jasperreports.engine.base.ElementsBlock.add(VirtualizableElementList.java:339)
        at net.sf.jasperreports.engine.base.ElementsBlockList.add(VirtualizableElementList.java:754)
        at net.sf.jasperreports.engine.base.VirtualizableElementList.add(VirtualizableElementList.java:124)
        at net.sf.jasperreports.engine.base.JRVirtualPrintPage.addElement(JRVirtualPrintPage.java:142)
        at net.sf.jasperreports.engine.fill.JRBaseFiller.fillBand(JRBaseFiller.java:1295)
        at net.sf.jasperreports.engine.fill.JRVerticalFiller.fillColumnBand(JRVerticalFiller.java:2458)
        at net.sf.jasperreports.engine.fill.JRVerticalFiller.fillDetail(JRVerticalFiller.java:761)
        at net.sf.jasperreports.engine.fill.JRVerticalFiller.fillReportContent(JRVerticalFiller.java:260)
        at net.sf.jasperreports.engine.fill.JRVerticalFiller.fillReport(JRVerticalFiller.java:103)
        at net.sf.jasperreports.engine.fill.JRBaseFiller.fill(JRBaseFiller.java:607)
        at net.sf.jasperreports.engine.fill.BaseReportFiller.fill(BaseReportFiller.java:405)
        at net.sf.jasperreports.engine.fill.JRFillSubreport.fillSubreport(JRFillSubreport.java:749)
        at net.sf.jasperreports.engine.fill.JRSubreportRunnable.run(JRSubreportRunnable.java:59)
        at net.sf.jasperreports.engine.fill.AbstractThreadSubreportRunner.run(AbstractThreadSubreportRunner.java:221)
        at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
        at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
        at java.lang.Thread.run(Thread.java:748)
Caused by: java.io.IOException: Stream Closed
        at java.io.RandomAccessFile.seek0(Native Method)
        at java.io.RandomAccessFile.seek(RandomAccessFile.java:557)
        at net.sf.jasperreports.engine.util.JRSwapFile.write(JRSwapFile.java:173)
        at net.sf.jasperreports.engine.util.JRSwapFile.write(JRSwapFile.java:162)
        at net.sf.jasperreports.engine.util.SwapFileVirtualizerStore.store(SwapFileVirtualizerStore.java:106)
        ... 23 more

 * 
 * In our environments, clients for example would have 10 reports scheduled for the early morning. 
 * Intermittently, 1 or 2 reports would fail to generate.
 * One workaround is to spread out the schedules so they are not overlapping.
 * However at some point this does not scale.
 * For example, if a report take a long time, it can run into the next schedule. 
 * Or For example, if 20 reports that each take 15 minutes are needed by 5am on a reporting schema finalized at 1am, overlapping report generation is needed.
 * 
 * I believe the intent of the system is to support many overlapping report schedules, parallel generation of reports, taking
 * advantage of the 'shared' swap file system to efficiently manage resources during this.
 * 
 * To reproduce the error, in jasper, 
 *  - configure multiple large adhoc reports producing excel file outputs
 *  - configure a schedule for these reports that overlap
 *  
 * In my test instance, my reproduction includes
 *  - 3 adhoc reports, each producing excel output files of 20 - 50 MB
 *  - A recurring schedule for each at 10-20 minute intervals
 *  - This setup produces the 'Stream Closed' error after 1-2 hours almsot every time
 * 
 * The issue could be described as an issue in the usage of the net.sf.jasperreports APIs by the com.jaspersoft.jasperserver package.
 * However this is the internal tibco code, and it would be nice for this package to be 'safe' from misuse.
 * So a proposed fix here might not the right place, but maybe could be considered as an attempt to make incorrect usage safe. 
 * 
 * The issue comes about from the 'ReportOptionsExecutionJob and 'ReportExecutionJob' of com.jaspersoft.jasperserver.
 * The report execution is split between quartz job threads and thread pools, and it ends up that a report runner thread 
 * attempts to pageOut to a swap file that has been closed by a quartz worker thread.
 * There is definitely some fixes required in this flow, and the sequence of generating a report and disposing the items.
 * While one thread disposes a store for a context, another thread later attempts to pageOut data using the same context, and same store. 
 * This results in a report generation error since the underlying swap file has been disposed (closed, and deleted).
 * 
 * However the attempt is to keep the scope of the fix inside net.sf.jasperreports, and to make these apis failsafe.
 * I assume the usage of shared virtualizer stores should be multi-threaded, and thread safe. 
 * I also assume the intent is of a shared swap file virtualizer is for a swap file to be thread safe,
 * be shared amongst many reports, and support multiple concurrent reports reading a writing from it (shared data between reports),
 * and hence intends to be only disposed once all the shared reports are done with it.  
 * 
 * A few possible fixes that proved to at least fix the issue are
 *  - only dispose if other contexts do not reference the same store (in the contexts to store map)?
 *  - don't dispose, rely on the weak references and finalize of VM
 *  - remove disposed stores from the contexts to store map (so new requests don't get disposed stores)
 *  - if a disposed store is retrieved from the store map, reject it and create a new store (but wont work with a pageIn call...)
 *  - only dispose if there are no 'handles' in the SwapFileVirtualizerStore before disposing
 *  
 * Tests are added here to reproduce concurrency issues, and ensure the newly introduced fixes work.
 * We assume pageIn/pageOut/dispose should be thread safe and can be called in any order.
 * This is probably not the expected api usage, but since dispose is getting called before pageIn, we want to
 * at least support thread safe behavior as must as possible to guard against even misuse.
 */
public class StoreFactoryVirtualizerTest extends AbstractTest
{


	/**
	 * This is a basic reproduction of what is happening in the jasper server.
	 * A dispose is called, and then a pageOut is called with the same context.
	 * This gives a JRRuntimeException caused by an IOException (Stream Closed), which
	 * flows out to cause the JobExecutionException and a failure to generate the report.
	 * 
	 * This could be considered invalid usage. But the error given as a result is not indicative
	 * of the usage error. For example, the error should be something like 'The store has already been
	 * disposed for this context, check to not reuse this context after the dispose call has been given for
	 * this context'. Also the javadoc is not clear about how to use dispose, since it idoes not mention how or
	 * when the underlying file store will be closed, and internally uses an 'owner' flag to determine if it 
	 * will be closed or not, and not clear how or who should be setting owner to false (it seems to be always true).
	 * 
	 * This produces the same JRRuntimeException caused by IOException happening at the jasper server runtime for
	 * scheduled reports.
	 * 
	 * It would be nice to be able to continue paging into and out of a new swap file transparently even
	 * if someone has requested disposal, rather than breaking the jasper runtime with a report failure. 
	 * @throws Exception
	 */
	@Test
	public void testDisposeThenPageOut() throws Exception
	{
		SwapFileVirtualizerStoreFactory swapFactory = new SwapFileVirtualizerStoreFactory();
		StoreFactoryVirtualizer sfv = new StoreFactoryVirtualizer(300, swapFactory);
		
		SimpleJasperReportsContext jasperReportsContext = new SimpleJasperReportsContext();
		JRVirtualizationContext vc = new JRVirtualizationContext(jasperReportsContext);
		JRVirtualPrintPage page = new JRVirtualPrintPage(vc); 
		ElementsBlock block1 = new ElementsBlock(vc, page);
		
		// page out
		sfv.pageOut(block1);
		
		// dispose
		sfv.dispose(vc);

		// page out a new block after dispose
		ElementsBlock block2 = new ElementsBlock(vc, page);
		sfv.pageOut(block2);

		// cleanup
		sfv.dispose(vc);
	}

	/**
	 * This is a basic test of pageOut and pageIn
	 * A block is paged out and paged back in, then another block is paged out and paged back in
	 * @throws Exception
	 */
	@Test
	public void testPageOutPageIn() throws Exception
	{
		SwapFileVirtualizerStoreFactory swapFactory = new SwapFileVirtualizerStoreFactory();
		StoreFactoryVirtualizer sfv = new StoreFactoryVirtualizer(300, swapFactory);
		
		SimpleJasperReportsContext jasperReportsContext = new SimpleJasperReportsContext();
		JRVirtualizationContext vc = new JRVirtualizationContext(jasperReportsContext);
		JRVirtualPrintPage page = new JRVirtualPrintPage(vc); 
		ElementsBlock block1 = new ElementsBlock(vc, page);
		JasperPrint print = new JasperPrint();
		JRBasePrintText text = new JRBasePrintText(print.getDefaultStyleProvider());
		text.setText("test1");
		block1.add(text);
		block1.beforeExternalization();

		sfv.pageOut(block1);
		sfv.pageIn(block1);

		ElementsBlock block2 = new ElementsBlock(vc, page);
		JRBasePrintText text2 = new JRBasePrintText(print.getDefaultStyleProvider());
		text2.setText("test2");
		block2.add(text2);
		block2.beforeExternalization();

		sfv.pageOut(block2);
		sfv.pageIn(block2);

		// cleanup
		sfv.dispose(vc);
	}

	/**
	 * This is a basic reproduction of what is happening in the jasper server.
	 * A dispose is called, and then a pageOut is called with the same context.
	 * This gives a JRRuntimeException caused by an IOException (Stream Closed), which
	 * flows out to cause the JobExecutionException and a failure to generate the report.
	 * 
	 * This could be considered invalid usage. But the error given as a result is not indicative
	 * of the usage error. For example, the error should be something like 'The store has already been
	 * disposed for this context, check to not reuse this context after the dispose call has been given for
	 * this context'. Also the javadoc is not clear about how to use dispose, since it idoes not mention how or
	 * when the underlying file store will be closed, and internally uses an 'owner' flag to determine if it 
	 * will be closed or not, and not clear how or who should be setting owner to false (it seems to be always true).
	 * 
	 * This produces the same JRRuntimeException caused by IOException happening at the jasper server runtime for
	 * scheduled reports.
	 * 
	 * It would be nice to be able to continue paging into and out of a new swap file transparently even
	 * if someone has requested disposal, rather than breaking the jasper runtime with a report failure. 
	 * @throws Exception
	 */
	@Test
	public void testDisposeThenPageOutPageIn() throws Exception
	{
		SwapFileVirtualizerStoreFactory swapFactory = new SwapFileVirtualizerStoreFactory();
		StoreFactoryVirtualizer sfv = new StoreFactoryVirtualizer(300, swapFactory);
		
		SimpleJasperReportsContext jasperReportsContext = new SimpleJasperReportsContext();
		JRVirtualizationContext vc = new JRVirtualizationContext(jasperReportsContext);
		JRVirtualPrintPage page = new JRVirtualPrintPage(vc);
		ElementsBlock block1 = new ElementsBlock(vc, page);
		JasperPrint print = new JasperPrint();
		JRBasePrintText text = new JRBasePrintText(print.getDefaultStyleProvider());
		text.setText("test1");
		block1.add(text);
		block1.beforeExternalization();
		
		// page out / page in
		sfv.pageOut(block1);
		sfv.pageIn(block1);

		// dispose
		sfv.dispose(vc);

		// page out / page in new block after dispose
		ElementsBlock block2 = new ElementsBlock(vc, page);
		JRBasePrintText text2 = new JRBasePrintText(print.getDefaultStyleProvider());
		text2.setText("test2");
		block2.add(text2);
		block2.beforeExternalization();
		sfv.pageOut(block2);
		sfv.pageIn(block2);

		// cleanup
		sfv.dispose(vc);
	}


	/**
	 * A dispose is called (maybe by some other thread), and then a pageIn is called for a handle (uid)
	 * that has been put in the swap with the same context.
	 * Rather than fail, it would be nice to support this if neccessary.
	 * The client could/should be at fault with incorrect usage, but without the swap configuration
	 * could support this by a mode where it will only remove the swap if handles are empty.
	 * Ie. All stores have been followed up with retrieve/remove.
	 * 
	 * A proposed configuration on the SwapFileVirtualizerStoreFactory (update to application.xml) could 
	 * configure the jasper instance in this way to prevent possible report failures.
	 * 
	 * The default should still be the existing behavior, where a caller calling dispose will be honored with
	 * the actual disposal, which also clears all the handles under management.
	 * 
	 * If this is occurring, a proposed flag 'disposeOnlyWithEmptyHandles' should be used in applicationContext.xml
	 * on the SwapFileVirtualizerStoreFactory. Maybe not a good or recommended solution, but will allow pageIn to happen
	 * after a thread might call dispose too early. This may delay or prevent disposal through, and implies clients should
	 * always pageIn after pageOut like a stack.
	 * @throws Exception
	 */
	@Test
	public void testDisposeThenPageIn() throws Exception
	{
		SwapFileVirtualizerStoreFactory swapFactory = new SwapFileVirtualizerStoreFactory();
		swapFactory.setDisposeOnlyWithEmptyHandles(true);
		StoreFactoryVirtualizer sfv = new StoreFactoryVirtualizer(300, swapFactory);
		
		SimpleJasperReportsContext jasperReportsContext = new SimpleJasperReportsContext();
		JRVirtualizationContext vc = new JRVirtualizationContext(jasperReportsContext);
		JRVirtualPrintPage page = new JRVirtualPrintPage(vc);
		ElementsBlock block1 = new ElementsBlock(vc, page);
		JasperPrint print = new JasperPrint();
		JRBasePrintText text = new JRBasePrintText(print.getDefaultStyleProvider());
		text.setText("test1");
		block1.add(text);
		block1.beforeExternalization();
		
		// page out
		sfv.pageOut(block1);

		// dispose
		sfv.dispose(vc);

		// pageIn
		sfv.pageIn(block1);
		
		// cleanup
		sfv.dispose(vc);
	}

	/**
	 * A multi-threaded pageIn and pageOut use case.
	 * 
	 * This case was broken before due to two issues:
	 *  
	 * a) the check for and subsequent and construction of the store for a context was not thread safe in StoreFactoryVirtualizer
	 * In concurrent use, threads calling pageOut could create a new swap for the context, and replace out the existing swap, 
	 * losing a handle for pageOut and causing the pageIn to fail.
	 * 
	 * The error given in the pageIn call is:
	 * 
	 * ERROR util.SwapFileVirtualizerStore - No swap handle found for <uid> in SwapFileVirtualizerStore JRSwapFile <swapfile>
	 *   net.sf.jasperreports.engine.JRRuntimeException: Unable to read virtualized data.
	 *   at net.sf.jasperreports.engine.util.SwapFileVirtualizerStore.retrieve(SwapFileVirtualizerStore.java:163)
	 *   at net.sf.jasperreports.engine.fill.StoreFactoryVirtualizer.pageIn(StoreFactoryVirtualizer.java:143)
	 * 
	 * A lock is added into the store method which checks and constructs new stores for a context
	 * 
	 * b) the serializer used by the SwapFileVirtualizerStore is not thread safe
	 * incorrect bytes were serialized out under parallel use, causing the pageIn to fail
	 * A lock is added around the serializer.writeData to protect against this.
	 * 
	 * The error given in the pageIn call is:
	 * 
	 * ERROR  net.sf.jasperreports.engine.JRRuntimeException: Error devirtualizing object.
	 *   at net.sf.jasperreports.engine.util.SwapFileVirtualizerStore.retrieve(SwapFileVirtualizerStore.java:185)
	 *   at net.sf.jasperreports.engine.fill.StoreFactoryVirtualizer.pageIn(StoreFactoryVirtualizer.java:143)
	 * Caused by: java.io.InvalidClassException: Circular reference.
	 *   at java.io.ObjectStreamClass.getClassDataLayout0(ObjectStreamClass.java:1327)
	 *   at java.io.ObjectStreamClass.getClassDataLayout(ObjectStreamClass.java:1307)
	 *   at java.io.ObjectInputStream.readSerialData(ObjectInputStream.java:2161)
	 *   at java.io.ObjectInputStream.readOrdinaryObject(ObjectInputStream.java:2069)
	 *   at java.io.ObjectInputStream.readObject0(ObjectInputStream.java:1573)
	 *   at java.io.ObjectInputStream.readObject(ObjectInputStream.java:431)
	 *   at net.sf.jasperreports.engine.virtualization.VirtualizationInput.readJRObject(VirtualizationInput.java:84)
	 *   at net.sf.jasperreports.engine.virtualization.VirtualizationInput.readJRObject(VirtualizationInput.java:69)
	 *   at net.sf.jasperreports.engine.base.VirtualElementsData.readVirtualized(VirtualElementsData.java:179)
	 *   at net.sf.jasperreports.engine.virtualization.SerializableSerializer.read(SerializableSerializer.java:131)
	 *   at net.sf.jasperreports.engine.virtualization.SerializableSerializer.read(SerializableSerializer.java:1)
	 *   at net.sf.jasperreports.engine.virtualization.VirtualizationInput.readJRObject(VirtualizationInput.java:105)
	 *   at net.sf.jasperreports.engine.virtualization.VirtualizationInput.readJRObject(VirtualizationInput.java:69)
	 *   at net.sf.jasperreports.engine.util.VirtualizationSerializer.readData(VirtualizationSerializer.java:67)
	 *   at net.sf.jasperreports.engine.util.VirtualizationSerializer.readData(VirtualizationSerializer.java:60)
	 *   at net.sf.jasperreports.engine.util.SwapFileVirtualizerStore.retrieve(SwapFileVirtualizerStore.java:178)
	 *   ... 5 more
	 *   
	 * This test simulates parallel writes, then parallel reads for blocks from the virtualizer.
	 *  
	 * @throws Exception
	 */
	@Test
	public void testPageOutConcurrent() throws Exception
	{
		SwapFileVirtualizerStoreFactory swapFactory = new SwapFileVirtualizerStoreFactory();
		swapFactory.setDisposeOnlyWithEmptyHandles(true);
		StoreFactoryVirtualizer sfv = new StoreFactoryVirtualizer(300, swapFactory);
		
		SimpleJasperReportsContext jasperReportsContext = new SimpleJasperReportsContext();
		JRVirtualizationContext vc = new JRVirtualizationContext(jasperReportsContext);
		JRVirtualPrintPage page = new JRVirtualPrintPage(vc);
		JasperPrint print = new JasperPrint();
		

		// threads
		int threads = 10;
		int timeoutMinutes = 2;
		
		// create blocks
		List<ElementsBlock> blocks = new ArrayList<>(threads);
		for(int i = 0; i < threads; i++) {
			ElementsBlock block1 = new ElementsBlock(vc, page);
			JRBasePrintText text = new JRBasePrintText(print.getDefaultStyleProvider());
			text.setText("test" + i);
			block1.add(text);
			block1.beforeExternalization();
			blocks.add(block1);
		}
		
		
		// parallel pageOut of all blocks
		ExecutorService e = Executors.newFixedThreadPool(threads);
		
		AtomicInteger pageOutSuccess = new AtomicInteger();
		List<Exception> pageOutErrors = new ArrayList<>(); 
		
		for(int i = 0; i < blocks.size(); i++) {
			final ElementsBlock block = blocks.get(i);
			e.execute(new Runnable() {
				@Override public void run () 
				{
					try 
					{
						sfv.pageOut(block);
						pageOutSuccess.incrementAndGet();
					} 
					catch(Exception ex) 
					{
						pageOutErrors.add(ex);
					}
				}
			});
		}
		e.shutdown();
		boolean t = e.awaitTermination(timeoutMinutes, TimeUnit.MINUTES);
		Assert.assertTrue(t, "timed out writing blocks");

		// verify
		Assert.assertEquals(pageOutSuccess.get(), blocks.size(), "should have successfully done all pageOut calls, errors: " + pageOutErrors);

		
		// parallel pageIn of all blocks
		e = Executors.newFixedThreadPool(threads);

		AtomicInteger pageInSuccess = new AtomicInteger();
		List<Exception> pageInErrors = new ArrayList<>(); 

		for(int i = 0; i < blocks.size(); i++) {
			final ElementsBlock block = blocks.get(i);
			e.execute(new Runnable() {
				@Override public void run () 
				{
					try 
					{
						sfv.pageIn(block);
						pageInSuccess.incrementAndGet();
					} 
					catch(Exception ex) 
					{
						pageInErrors.add(ex);
					}
				}
			});
		}
		e.shutdown();
		t = e.awaitTermination(timeoutMinutes, TimeUnit.MINUTES);
		Assert.assertTrue(t, "timed out writing blocks");

		Assert.assertEquals(pageInSuccess.get(), blocks.size(), "should have successfully done all pageOut calls, errors: " + pageOutErrors);

		// cleanup
		sfv.dispose(vc);
	}

	/**
	 * A multi-threaded dispose / pageIn / pageOut use case.
	 * The keeps pageIn and pageOut calls sequential, but inserts the dispose calls as well.
	 * It is still not clear if this use case should be invalid, but since we see dispose calls being done in
	 * jasper server before pageIn calls, we test this.
	 * The goal is to make the swap store thread safe in read/write/dispose.
	 * 
	 * Note that this test still needs the new optional flag: setDisposeOnlyWithEmptyHandles=true
	 * Since dispose is called in parallel, it can dispose in between a pageOut and pageIn causing the pageIn to fail.
	 * 
	 * I did not want the context lock in the pageIn and pageOut, however it seems the simplest and best way to achieve the 
	 * lock that protect the operations in parallel with a dispose. There maybe some loss of parallelism here for a context, but I am 
	 * assuming the parallelism should be related to multiple different reports, not the same report, and hence not the same context.
	 * I would think for the same context, in general the report construction would be generally serial, such as pageOut,pageIn,Dispose.
	 * The parallelism should be most beneficial to multiple different reporting running that have different contexts, so a context level
	 * lock is is good.
	 * 
	 * This shows the lock must be around using the store and disposing the store, not just the retrieval or getting the store.
	 * In a concurrent reading write and dispose scenario, the following error occurs
	 * even with the internal fixes for above tests.
	 * This is since the existing locking is around retrieval, but the disposal and the actual reading and writing occur
	 * concurrently, so both pageOut and pageIn calls fail
	 * 
	 * For pageOut in parallel with a dispose: 
	 * ERROR util.SwapFileVirtualizerStore - Error virtualizing object <uid> to JRSwapFile <file>
	 *  java.io.IOException: Stream Closed
	 *  at java.io.RandomAccessFile.length(Native Method) ~[?:1.8.0_191]
	 *  at net.sf.jasperreports.engine.util.JRSwapFile.reserveFreeBlocks(JRSwapFile.java:297) ~[classes/:?]
	 *  at net.sf.jasperreports.engine.util.JRSwapFile.write(JRSwapFile.java:157) ~[classes/:?]
	 *  at net.sf.jasperreports.engine.util.SwapFileVirtualizerStore.store(SwapFileVirtualizerStore.java:136) [classes/:?]
	 *  at net.sf.jasperreports.engine.fill.StoreFactoryVirtualizer.pageOut(StoreFactoryVirtualizer.java:134) [classes/:?]
	 *  at net.sf.jasperreports.engine.fill.StoreFactoryVirtualizerTest$3.run(StoreFactoryVirtualizerTest.java:499) [test-classes/:?]
	 *
	 * For pageIn in parallel with a dispose
	 * ERROR util.SwapFileVirtualizerStore - No swap handle found for <uid> in SwapFileVirtualizerStore JRSwapFile <file>
	 *   net.sf.jasperreports.engine.JRRuntimeException: Unable to read virtualized data.
	 *   at net.sf.jasperreports.engine.util.SwapFileVirtualizerStore.retrieve(SwapFileVirtualizerStore.java:161)
	 *   at net.sf.jasperreports.engine.fill.StoreFactoryVirtualizer.pageIn(StoreFactoryVirtualizer.java:161)
	 *   at net.sf.jasperreports.engine.fill.StoreFactoryVirtualizerTest$3.run(StoreFactoryVirtualizerTest.java:510)
	 *   at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
	 *   at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
	 *   at java.lang.Thread.run(Thread.java:748)
	 *
	 *
	 * @throws Exception
	 */
	@Test(invocationCount = 20)
	public void testConcurrency() throws Exception
	{
		SwapFileVirtualizerStoreFactory swapFactory = new SwapFileVirtualizerStoreFactory();
		swapFactory.setDisposeOnlyWithEmptyHandles(true);
		StoreFactoryVirtualizer sfv = new StoreFactoryVirtualizer(300, swapFactory);
		
		SimpleJasperReportsContext jasperReportsContext = new SimpleJasperReportsContext();
		JRVirtualizationContext vc = new JRVirtualizationContext(jasperReportsContext);
		JRVirtualPrintPage page = new JRVirtualPrintPage(vc);
		JasperPrint print = new JasperPrint();
		

		// threads
		int threads = 10;
		int objects = 1000;
		int timeoutMinutes = 5;
		
		// create blocks
		List<ElementsBlock> blocks = new ArrayList<>(objects);
		for(int i = 0; i < objects; i++) {
			ElementsBlock block1 = new ElementsBlock(vc, page);
			JRBasePrintText text = new JRBasePrintText(print.getDefaultStyleProvider());
			text.setText("test" + i);
			block1.add(text);
			block1.beforeExternalization();
			blocks.add(block1);
		}
		
		
		// parallel pageOut of all blocks
		ExecutorService e = Executors.newFixedThreadPool(threads);
		
		AtomicInteger pageOutSuccess = new AtomicInteger();
		AtomicInteger pageInSuccess = new AtomicInteger();
		AtomicInteger disposeSuccess = new AtomicInteger();
		List<Exception> pageOutErrors = new ArrayList<>(); 
		List<Exception> pageInErrors = new ArrayList<>(); 
		List<Exception> disposeErrors = new ArrayList<>(); 
		
		for(int i = 0; i < blocks.size(); i++) {
			final ElementsBlock block = blocks.get(i);
			e.execute(new Runnable() {
				@Override public void run () 
				{
					try 
					{
						sfv.pageOut(block);
						pageOutSuccess.incrementAndGet();
					}	
					catch(Exception ex) 
					{
						pageOutErrors.add(ex);
						ex.printStackTrace();
					}
					try 
					{
						Thread.sleep(100);
						sfv.pageIn(block);
						pageInSuccess.incrementAndGet();
					} 
					catch(Exception ex) 
					{
						pageInErrors.add(ex);
					}
				}
			});
			e.execute(new Runnable() {
				@Override public void run () 
				{
					try 
					{
						sfv.dispose(vc);
						disposeSuccess.incrementAndGet();
					} 
					catch(Exception ex) 
					{
						disposeErrors.add(ex);
					}
				}
			});
		}
		e.shutdown();
		boolean t = e.awaitTermination(timeoutMinutes, TimeUnit.MINUTES);
		Assert.assertTrue(t, "timed out writing blocks");

		// verify
		Assert.assertEquals(pageOutSuccess.get(), blocks.size(), "should have successfully done all pageOut calls, errors: " + pageOutErrors);
		Assert.assertEquals(pageInSuccess.get(), blocks.size(), "should have successfully done all pageIn calls, errors: " + pageOutErrors);
		Assert.assertEquals(disposeSuccess.get(), blocks.size(), "should have successfully done all dispose calls, errors: " + disposeErrors);

		// pageIns expect some errors
		
		// cleanup
		sfv.dispose(vc);
	}
}
