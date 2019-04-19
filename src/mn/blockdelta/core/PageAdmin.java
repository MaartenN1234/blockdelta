package mn.blockdelta.core;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import mn.blockdelta.core.conversions.RowsourceGenerator;
import mn.blockdelta.core.conversions.RowsourceHeader;
import mn.blockdelta.core.scheduler.WorkloadQueue;
import mn.blockdelta.core.scheduler.WorkloadTask;


public class PageAdmin {
	public final static String PAGE_KEY_SPLIT_CHARACTER = String.valueOf((char) 0xEFFF);
	
	/* Fundamentally we need the following data
	 * outputDataHash             --> check output data changed
	 * inputDataHash              --> check input data has changed
	 * lastSyncedRequestTimeStamp --> shortcut double evaluations
	 * pageKey                    --> do actual work with
	 */
	public long   outputDataHash;
	public long   inputDataHash ;
	public long   lastSyncedRequestTimeStamp;
	public String pageKey;
	
	
	private transient RowsourceGenerator rowsourceGenerator;
	private transient String []          rowsourcePageKeys;


	
	public PageAdmin(String ... pageKeys){
		this.pageKey        = String.join(PAGE_KEY_SPLIT_CHARACTER, pageKeys);
		inputDataHash       = 0; 
		outputDataHash      = 0;
		lastSyncedRequestTimeStamp = 0;
	}
	
	public RowsourceGenerator getRowsourceGenerator(){
		if (rowsourceGenerator == null){
			String rowsourceName = pageKey.split(PAGE_KEY_SPLIT_CHARACTER)[0];
			rowsourceGenerator   = MasterAdministration.getRowsourceGenerator(rowsourceName);
		}
		return rowsourceGenerator;
	}
	private String [] getRowsourcePageKeys(){
		if (rowsourcePageKeys == null){
			String rowsourcePageKey = pageKey.substring(pageKey.indexOf(PAGE_KEY_SPLIT_CHARACTER)+1);
			rowsourcePageKeys       = rowsourcePageKey.split(PAGE_KEY_SPLIT_CHARACTER);
		}	
		return rowsourcePageKeys;
	}
	public Set<String> getInputPageKeysConfig(){
		return getRowsourceGenerator().getInputPageKeysConfig(getRowsourcePageKeys());
	}

	
	public Set<String> getInputPageKeysActual(Map<String, PageData> configInputPageData){
		return getRowsourceGenerator().getInputPageKeysActual(getRowsourcePageKeys(), configInputPageData);
	}
	
	public String toString(){
		return "Pageadmin for ("+pageKey+")";
	}
	
	public void dumpCurrentContents(){
		RowsourceHeader rowsourceHeader = getRowsourceGenerator().getRowsourceHeader();
		
		PageData pageData = PageMapProvider.retrieveData(pageKey);
		if (pageData == null){
			System.out.println("Page " +pageKey + " does not exist");
			return;
		}
		
		System.out.println(Arrays.toString(rowsourceHeader.getColumnNames()));

		pageData
			.streamUsingHeader(rowsourceHeader)
			.forEach(t -> System.out.println(t.toString()));
	}	
	
	public void deepSynchronize(Map<String, PageData> inputPageData, long requestTimeStamp, long currentInputDataHash) {
		PageData outputPageData = getRowsourceGenerator()
				.execute(getRowsourcePageKeys(), inputPageData);

		// Whenever output is new, change administration and store admin and output
		if (outputDataHash != outputPageData.hash){
			outputDataHash      = outputPageData.hash;
			inputDataHash       = currentInputDataHash; 
			lastSyncedRequestTimeStamp = requestTimeStamp;

			PageMapProvider.store(pageKey, this, outputPageData);
		}	else {
			lastSyncedRequestTimeStamp = requestTimeStamp; 			
			PageMapProvider.store(pageKey, this);
		}
	}

	public void ensurePageIsSynced(int basePriority, long requestTimeStamp) throws InterruptedException {
		WorkloadTask task = new WorkloadTask(pageKey, basePriority * 10000000, requestTimeStamp);
		WorkloadQueue.queue.waitUntilTaskCompletion(task);		
	}
}
