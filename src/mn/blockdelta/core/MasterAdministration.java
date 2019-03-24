package mn.blockdelta.core;

import java.util.HashMap;
import java.util.Map;

import mn.blockdelta.core.conversions.RowsourceGenerator;

public class MasterAdministration {
	private static MasterAdministration admin = new MasterAdministration();

	private long  							lastInvalidatedExternalSync;
	private Map<String, RowsourceGenerator> dataDictionary;
	
	private MasterAdministration(){		
		lastInvalidatedExternalSync = 1;
		dataDictionary              = new HashMap<String, RowsourceGenerator>();
	}
	


	/**
	 * @return the lastInvalidatedExternalSync
	 */
	public static long getLastInvalidatedExternalSync() {
		return admin.lastInvalidatedExternalSync;
	}
	/**
	 * @param lastInvalidatedExternalSync the lastInvalidatedExternalSync to set
	 */
	public static void setLastInvalidatedExternalSync(long lastInvalidatedExternalSync) {
		admin.lastInvalidatedExternalSync = lastInvalidatedExternalSync;
	}
	
	public static void registerRowsourceGenerator(String name, RowsourceGenerator rowsourceGenerator){
		admin.dataDictionary.put(name, rowsourceGenerator);
	}
	public static RowsourceGenerator getRowsourceGenerator(String name){
		RowsourceGenerator result = admin.dataDictionary.get(name);
		if (result == null) 
			throw new RuntimeException("RowsourceGenerator "+ name + " was not registered.");
		return result;
	}	

	public static long getTimeStamp() {
		return System.currentTimeMillis();
	}	
}
