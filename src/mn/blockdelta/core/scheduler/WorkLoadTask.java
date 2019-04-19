package mn.blockdelta.core.scheduler;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import mn.blockdelta.core.MasterAdministration;
import mn.blockdelta.core.PageAdmin;
import mn.blockdelta.core.PageData;
import mn.blockdelta.core.PageMapProvider;

public class WorkloadTask {
	final static int STAGE_QUEUE_PREDECESSORS      =  0;
	final static int STAGE_SHALLOW_EVALUATE_STAGE1 = 10;
	final static int STAGE_SHALLOW_EVALUATE_STAGE2 = 12;
	final static int STAGE_DEEP_EVALUATE           = 20;
	final static int STAGE_FINALIZED               = 30;
	
	final String      pageKey;
	final int         priority;
	final long        requestTimeStamp;      
	int               stage;
	Set<WorkloadTask> waitForSet;
	
	public WorkloadTask (String pageKey, int priority, long requestTimeStamp){
		this.pageKey          = pageKey;
		this.priority         = priority;
		this.requestTimeStamp = requestTimeStamp;
		this.stage            = STAGE_QUEUE_PREDECESSORS;
		this.waitForSet       = null;
	}
	
	boolean isFastStage(){
		return     stage == STAGE_QUEUE_PREDECESSORS
				|| stage == STAGE_SHALLOW_EVALUATE_STAGE1
				|| stage == STAGE_SHALLOW_EVALUATE_STAGE2;		
	}
	public boolean isFinalized() {
		return stage == STAGE_FINALIZED;
	}	
	
	public boolean equals (Object o){
		if (o instanceof WorkloadTask){
			WorkloadTask t = (WorkloadTask) o;
			return t.pageKey.equals(pageKey) 
					&& t.requestTimeStamp == requestTimeStamp;
		}
		return false;
	}
	public int hashCode(){
		return  pageKey.hashCode() ^ ((int) (requestTimeStamp & 0xFFFF));
	}

	private void collectInputKeysInWaitForSet(Set<String> inputKeys, boolean highPrio){
		if (inputKeys == null)
			return;
		if (waitForSet == null)
			waitForSet = new HashSet<WorkloadTask>();
		
		inputKeys
		.stream()
		.map(t -> new WorkloadTask (t, this.priority + (highPrio ? 1000 : 1), this.requestTimeStamp))
		.forEach(t -> waitForSet.add(t));
	}
	private Map<String, PageData> collectPageDataForInputKeys(Set<String> inputKeys){
		if (inputKeys == null)
			return null;
		
		return  inputKeys
				.stream()
				.collect(Collectors.toMap(t -> t, 
										  t -> PageMapProvider.retrieveData(t)));		
	}	

	private long calculateInputDataHash(PageAdmin  pageAdmin, Set<String> inputKeys){	
		long currentInputDataHash = 0;
		if (inputKeys != null){
			currentInputDataHash = inputKeys
				.stream() 
				.map(t -> PageMapProvider.retrieveAdmin(t))
				.sorted((l,r) -> l.outputDataHash > r.outputDataHash ? 1 : l.outputDataHash == r.outputDataHash ? 0 : -1)
				.map(t -> t.outputDataHash) // get Hash per element 
				.reduce((aggrHash, elementHash) -> 31 * aggrHash + elementHash) // Aggregate Hash
				.orElse(0l);
		} 
		
		if (pageAdmin.getRowsourceGenerator().usesExternalInput()){
			currentInputDataHash = currentInputDataHash * 31 + MasterAdministration.getLastInvalidatedExternalSync();
		}
		
		return  currentInputDataHash;		
	}		
	
	public void executeStage() {
		PageAdmin   			pageAdmin = PageMapProvider.retrieveAdmin(pageKey);
		if (pageAdmin.lastSyncedRequestTimeStamp >= requestTimeStamp){
			stage = STAGE_FINALIZED;
			return;
		}
		Set<String> 			inputPageKeys;
		Map<String, PageData>   inputPageData;
		long                    inputDataHash;
		
		switch(stage){
		case STAGE_QUEUE_PREDECESSORS:
			inputPageKeys = pageAdmin.getInputPageKeysConfig();
			if (inputPageKeys == null || inputPageKeys.isEmpty()){
				inputPageKeys = pageAdmin.getInputPageKeysActual(null);
				if (inputPageKeys != null && !inputPageKeys.isEmpty()){					
					collectInputKeysInWaitForSet(inputPageKeys, false);
				} 
				stage = STAGE_SHALLOW_EVALUATE_STAGE2;
			} else {
				collectInputKeysInWaitForSet(inputPageKeys, true);
				stage = STAGE_SHALLOW_EVALUATE_STAGE1;
			}			
			break;
		case STAGE_SHALLOW_EVALUATE_STAGE1:
			inputPageKeys = pageAdmin.getInputPageKeysConfig();
			inputPageData = collectPageDataForInputKeys(inputPageKeys);
			inputPageKeys = pageAdmin.getInputPageKeysActual(inputPageData);
			if (inputPageKeys != null && !inputPageKeys.isEmpty()){					
				collectInputKeysInWaitForSet(inputPageKeys, false);
			} 			
			stage = STAGE_SHALLOW_EVALUATE_STAGE2;
			break;
		case STAGE_SHALLOW_EVALUATE_STAGE2:
			inputPageKeys = pageAdmin.getInputPageKeysConfig();
			if (inputPageKeys != null && !inputPageKeys.isEmpty()){		
				inputPageData = collectPageDataForInputKeys(inputPageKeys);
				inputPageKeys = pageAdmin.getInputPageKeysActual(inputPageData);
			} else {
				inputPageKeys = pageAdmin.getInputPageKeysActual(null);
			}
			inputDataHash = calculateInputDataHash(pageAdmin, inputPageKeys);
			if (pageAdmin.inputDataHash == inputDataHash){
				stage = STAGE_FINALIZED;
			} else {
				stage = STAGE_DEEP_EVALUATE;
			}			
			break;
		case STAGE_DEEP_EVALUATE:
			inputPageKeys = pageAdmin.getInputPageKeysConfig();
			if (inputPageKeys != null && !inputPageKeys.isEmpty()){		
				inputPageData = collectPageDataForInputKeys(inputPageKeys);
				inputPageKeys = pageAdmin.getInputPageKeysActual(inputPageData);
			} else {
				inputPageKeys = pageAdmin.getInputPageKeysActual(null);
			}

			inputPageData = collectPageDataForInputKeys(inputPageKeys);
			inputDataHash = calculateInputDataHash(pageAdmin, inputPageKeys);
			pageAdmin.deepSynchronize(inputPageData, requestTimeStamp, inputDataHash);
			stage = STAGE_FINALIZED;
			break;
		case STAGE_FINALIZED:
			throw new IllegalStateException("A finalized WorkloadTask should not be executed.");
		default:
			throw new IllegalStateException("Unknown WorkloadTask stage.");
		}
	}

	public boolean isNotWaiting() {
		return waitForSet == null || waitForSet.isEmpty();
	}
	
	public String toString(){
		return pageKey + " (" + stage+")";
	}



	
	
}
