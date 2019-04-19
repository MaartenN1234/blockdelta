package mn.blockdelta.core.scheduler;

public class WorkloadThread extends Thread {
	public void run(){
		WorkloadTask task = WorkloadQueue.queue.getPrioritizedEligableTask();
		
		while (true){
			if (task != null){
				task.executeStage();
				WorkloadQueue.queue.markTaskComplete(task);
			} else {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			task = WorkloadQueue.queue.getPrioritizedEligableTask();
		}
	}

	public static void spawn() {
		(new WorkloadThread()).start();
	}
}
