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


public class PageAdmin {
	public final static String PAGE_KEY_SPLIT_CHARACTER = String.valueOf((char) 0xEFFF);
	
	/* Fundamentally we need the following data
	 * outputDataHash      --> check output data changed
	 * inputDataHash       --> check input data has changed
	 * lastSyncedTimeStamp --> shortcut double evaluations
	 * pageKey             --> do actual work with
	 */
	private long   outputDataHash;
	private long   inputDataHash ;
	private long   lastSyncedTimeStamp;
	private String pageKey;
	
	
	private transient RowsourceGenerator rowsourceGenerator;
	private transient String []          rowsourcePageKeys;


	
	public PageAdmin(String ... pageKeys){
		this.pageKey        = String.join(PAGE_KEY_SPLIT_CHARACTER, pageKeys);
		inputDataHash       = 0; 
		outputDataHash      = 0;
		lastSyncedTimeStamp = 0;
	}
	
	private RowsourceGenerator getRowsourceGenerator(){
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
	private Set<String> getInputPageDataKeys(){
		return getRowsourceGenerator().getInputPageKeysActual(getRowsourcePageKeys(), null);
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
	
	public void ensurePageIsSynced(int basePriority, long requestTimeStamp){
		// ShortCut lastSynced
		if (lastSyncedTimeStamp > requestTimeStamp)
			return;
		
		// Sync Input Pages and calculate currentInputDataHash
		long currentInputDataHash        = getInputPageDataKeys()
											.parallelStream() // Parallel stream to boost performance
											.map(t -> PageMapProvider.retrieveAdmin(t))  // Lookup Admin
											.map(t -> {t.ensurePageIsSynced(basePriority, requestTimeStamp); 
													   return t;}) // Assure Input pages are synced
											.collect(Collectors.toList())
											.stream() // Serial stream to have deterministic sorting order  
											.sorted((l,r) -> l.outputDataHash > r.outputDataHash ? 1 : l.outputDataHash == r.outputDataHash ? 0 : -1)
											.map(t -> t.outputDataHash) // get Hash per element 
											.reduce((aggrHash, elementHash) -> 31 * aggrHash + elementHash) // Aggregate Hash
											.orElse(0l);
		
		// If external input is required, add last invalidated time stamp to hash
		if (getRowsourceGenerator().usesExternalInput()){
			currentInputDataHash = currentInputDataHash * 31 + MasterAdministration.getLastInvalidatedExternalSync();
		}
							
	    // Trigger recalculation when needed
		if (currentInputDataHash != inputDataHash){
			// Trigger recalculation
			syncPage(basePriority, requestTimeStamp, currentInputDataHash);
		} else {
			// Log last sync, no need to update the output data
			lastSyncedTimeStamp = MasterAdministration.getTimeStamp(); 
			PageMapProvider.store(pageKey, this);
		}
		
	}

	private void syncPage(int basePriority, long requestTimeStamp, long currentInputDataHash) {
		// TODO this is a serial operation, it needs to become asynchronous.
		// TODO also it needs to be pruned if already completed or scheduled.

		synchonousSyncPage(requestTimeStamp, currentInputDataHash);
	}

	private void synchonousSyncPage(long requestTimeStamp, long currentInputDataHash) {
		synchronized (pageKey){
			// ShortCut lastSynced
			if (lastSyncedTimeStamp > requestTimeStamp)
				return;
			
			// Retrieve input data
			Map<String, PageData> pageData =  getInputPageDataKeys()
													.parallelStream()
													.collect(Collectors.toMap(t -> t, 
																			  t -> PageMapProvider.retrieveData(t)));
			
			// Calculate output data
			PageData outputPageData = getRowsourceGenerator()
										.execute(getRowsourcePageKeys(), pageData);
			
			
			// Whenever output is new, change administration and store admin and output
			if (outputDataHash != outputPageData.hash){
				outputDataHash      = outputPageData.hash;
				inputDataHash       = currentInputDataHash; 
				lastSyncedTimeStamp = MasterAdministration.getTimeStamp(); 
				
				PageMapProvider.store(pageKey, this, outputPageData);
			}
		}
	}
}
