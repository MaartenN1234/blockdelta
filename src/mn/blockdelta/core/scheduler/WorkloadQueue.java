package mn.blockdelta.core.scheduler;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import mn.blockdelta.util.MultiSet;

public class WorkloadQueue {
	MultiSet<WorkloadTask, WorkloadTask> taskList;
	Set<WorkloadTask>                    fastEligableTasks;
	PriorityQueue<WorkloadTask>          longEligableTasks;
	
	public static WorkloadQueue queue = new WorkloadQueue();
	private WorkloadQueue(){
		taskList          = new MultiSet<WorkloadTask, WorkloadTask>();
		fastEligableTasks = new HashSet<WorkloadTask>();
		longEligableTasks = new PriorityQueue<WorkloadTask>(1000, (t1, t2) -> t2.priority - t1.priority);
	}
	
	public synchronized void submitTask(WorkloadTask task, WorkloadTask subsequentTask){
		taskList.add(task, subsequentTask);

		if (task.isNotWaiting()){
			if (task.isFastStage()){
				fastEligableTasks.add(task);
			} else {
				longEligableTasks.add(task);
			}
		}
	}
	

	public synchronized WorkloadTask getPrioritizedEligableTask(){
		return longEligableTasks.poll();
	}
	
	public synchronized void markTaskComplete(WorkloadTask task){
		markTaskComplete(task, true);
	}
	private synchronized void markTaskComplete(WorkloadTask task, boolean propagateFastTasks){
		if (task.isNotWaiting()){
			if (task.isFinalized()){
				taskList
					.remove(task)
					.stream()
					.filter(t -> t != null)
					.map(t -> {t.waitForSet.remove(task); return t;})
					.filter(t -> t.isNotWaiting())
					.forEach(t -> submitTask(t, null));
			}
			else {
				submitTask(task, null);
			}
		} else {
			task.waitForSet.stream().forEach(t-> submitTask(t, task));
		} 
		if (propagateFastTasks)
			executeFastTasks();
	}
	
	private synchronized void executeFastTasks(){
		while (!fastEligableTasks.isEmpty()){
			WorkloadTask [] tasks = fastEligableTasks.toArray(new WorkloadTask[fastEligableTasks.size()]);
			fastEligableTasks.clear();
			for (WorkloadTask task : tasks){
				task.executeStage();
				markTaskComplete(task, false);
			}
		}		
	}
	private synchronized boolean taskHasCompleted(WorkloadTask task){
		return !taskList.containsKey(task);
	}

	public void waitUntilTaskCompletion(WorkloadTask task) throws InterruptedException {
		submitTask(task, null);
		executeFastTasks();
	
		while (!taskHasCompleted(task)){
			Thread.sleep(100);
		}
	}
	
}
