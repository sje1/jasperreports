package net.sf.jasperreports.engine.fill;

import java.io.File;
import java.io.FileWriter;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.text.SimpleDateFormat;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import net.sf.jasperreports.AbstractTest;
import net.sf.jasperreports.engine.JasperPrint;
import net.sf.jasperreports.engine.SimpleJasperReportsContext;
import net.sf.jasperreports.engine.base.ElementsBlock;
import net.sf.jasperreports.engine.base.JRBasePrintText;
import net.sf.jasperreports.engine.base.JRVirtualPrintPage;
import net.sf.jasperreports.engine.util.SwapFileVirtualizerStore;
import net.sf.jasperreports.engine.util.SwapFileVirtualizerStoreFactory;

/**
 * Just to look at pageIn and pageOut performance.
 * This was used to compare timing with various options to tweak performance of the SwapFileVirtualizerStore.
 * This was testing performance for a could of proposed changes.
 * No intended for a unit test run, and has no asserts. So could be excluded from the unit test suite, and later
 * modified to suite either a unit test or performance test suite.
 * 
 * - thread local reusable byte[] buffers : this can prevent a lot of creating and disposing byte arrays when both
 * reading and writing and serializing and deserializing. A thread local is nice here, assuming thread pools are used.
 * The assumption is that thread pool are used for all worker threads, scheduled reports, in jasper server, and seems
 * to be the case. Thread locals are cleaned up when threads end, so in the case of not using thread pools, it should not 
 * be worse than the existing implementation of creating each each time they are needed. This could have an impact on total
 * heap, but is also assumed to be minimal since also small thread pools are expected. So essentially caching arrays per thread
 * should not have much difference on the total heap than creating and disposing them, and is very helpful for GC.
 * The important note about using recycles byte array streams is never to take the length of data as the length of the byte array.
 *  
 * - some more closing of output streams in finally blocks : have to be a bit careful here, since some places pass around 
 * streams, so should not be closed 
 *  
 * - some low impact stat collection in SwapFileVirtualizerStore : this should be considered low impact, and usable in production.
 * It is off by default, but could be turned on and monitoring systems like over jmx could be used to get read and write performance
 * of the swap store over time. The stats here are just counters of bytes and counts, along with timing in nanos around the 
 * serialization/deserialization and file read/write operations. Initially added to test some code changes, it can be left in for 
 * production runtime performance, or could be removed. Other better ways such as performance agents can do a similar job without
 * embedded this into the codebase.
 * 
 * Results: 
 * With the 80k text and 200,000 pageIn and pageOut calls, with 25 runs
 * The proposed changes above could roughly generate a 5% improvement in total time and GC time
 * Most savings came from the total ending memory usage (14% better), deserialize time (6% better), and swap write time (21% better).
 * So all the proposed changes can be considered optional, or a starting point. 
 */
public class StoreFactoryVirtualizerVolumeTest extends AbstractTest
{
	int objects = 200000;
	int textsize = 80000;
	
	/**
	 * Will write out a CSV file to look at the stats.
	 */
	private File dataFile;
	
	@BeforeClass
	public void writeHeader() throws Exception {
		dataFile = new File("StoreFactoryVirtualizerVolumeTest.csv");
		try(FileWriter fw = new FileWriter(dataFile, true)) {
			fw.write("Time, Total Time (ns), GC Counts, GC Time (ms), Memory Used (bytes), Serialize Time (ns), Deserialize Time (ns), Swap Write Time (ns), Swap Read Time (ns)" + System.lineSeparator());
		}
	}

	@AfterClass
	public void end() {
		System.out.println("see data file " + dataFile.getAbsolutePath());
	}

	/**
	 * A test that just does a sequential pageIn and pageOut of data many times.
	 * The invocation count is used to get an average of many runs.
	 * @throws Exception
	 */
	@Test(invocationCount = 25)
	public void testSequentialPageInPageOut() throws Exception
	{
		SwapFileVirtualizerStoreFactory swapFactory = new SwapFileVirtualizerStoreFactory();
		swapFactory.setStatsEnabled(true);
		StoreFactoryVirtualizer sfv = new StoreFactoryVirtualizer(300, swapFactory);
		
		SimpleJasperReportsContext jasperReportsContext = new SimpleJasperReportsContext();
		JRVirtualizationContext vc = new JRVirtualizationContext(jasperReportsContext);
		JRVirtualPrintPage page = new JRVirtualPrintPage(vc);
		JasperPrint print = new JasperPrint();
		
		System.gc();
		Thread.sleep(5000);
		
		long start = System.nanoTime();
		long gcCounts = 0;
		long gcMillis = 0;
		for(GarbageCollectorMXBean gc: ManagementFactory.getGarbageCollectorMXBeans()) {
			long count = gc.getCollectionCount();
			if(count > 0) gcCounts += count;
			long millis = gc.getCollectionTime();
			if(millis > 0) gcMillis += millis;
		}
		
		long memUsed = 0L;
		for(MemoryPoolMXBean mem: ManagementFactory.getMemoryPoolMXBeans()) {
			long used = mem.getUsage().getUsed();
			if(used > 0) memUsed += used; 
		}

		// create blocks
		for(int i = 0; i < objects; i++) {
			ElementsBlock block1 = new ElementsBlock(vc, page);
			JRBasePrintText text = new JRBasePrintText(print.getDefaultStyleProvider());
			text.setText(makeText("0123456789", textsize/10));
			block1.add(text);
			block1.beforeExternalization();

			sfv.pageOut(block1);
			sfv.pageIn(block1);
		}

		long end = System.nanoTime();
		long endgcCounts = 0;
		long endgcMillis = 0;
		for(GarbageCollectorMXBean gc: ManagementFactory.getGarbageCollectorMXBeans()) {
			long count = gc.getCollectionCount();
			if(count > 0) endgcCounts += count;
			long millis = gc.getCollectionTime();
			if(millis > 0) endgcMillis += millis;
		}
		
		long endmemUsed = 0L;
		for(MemoryPoolMXBean mem: ManagementFactory.getMemoryPoolMXBeans()) {
			long used = mem.getUsage().getUsed();
			if(used > 0) endmemUsed += used; 
		}

		SwapFileVirtualizerStore store = (SwapFileVirtualizerStore)sfv.getExistingStore(vc);
		try(FileWriter fw = new FileWriter(dataFile, true)) {
			fw.write(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(new java.util.Date()) + ", " + (end-start) + ", " + (endgcCounts-gcCounts) + ", " + (endgcMillis-gcMillis) + ", " + (endmemUsed-memUsed) + ", " + store.getStoreStats().serializeNanos() + ", " + store.getStoreStats().deserializeNanos() + ", " + store.getStoreStats().writeNanos() + ", " + store.getStoreStats().readNanos() + System.lineSeparator());
		}
		
		// cleanup
		sfv.dispose(vc);
	}

	private String makeText(String s, int repeats) {
		StringBuilder sb = new StringBuilder();
		for(int i = 0; i < repeats; i++) {
			sb.append(s);
		}
		return sb.toString();
	}
}
