package mn.blockdelta.core;

import java.util.HashMap;
import java.util.Map;

public class PageMapProvider {
	/* TODO:
	 * These calls should be relayed to an effective cache/storage at a later stage, 
	 * now just use internal memory for tests */
	
	/* Administrative data */
	private static Map<String, PageAdmin> pageAdminMap = new HashMap <String, PageAdmin>();
	/* Actual data */
	private static Map<String, PageData>  pageDataMap  = new HashMap <String, PageData>();

	
	public static PageAdmin retrieveAdmin(String pageKey) {
		synchronized (pageAdminMap){
			PageAdmin result = pageAdminMap.get(pageKey);
			if (result == null){
				result = new PageAdmin(pageKey);
				pageAdminMap.put(pageKey, result);
			}
					
			return result;
		}
	}
	
	
	
	public static PageData retrieveData(String pageKey) {
		synchronized (pageAdminMap){
			PageData result = pageDataMap.get(pageKey);
			if (result == null){
				throw new RuntimeException("COULD NOT FIND DATA for " + pageKey);
			}
			return result;
		}
	}
	
	
	public static void store(String pageKey, PageAdmin pageAdmin, PageData pageData) {
		synchronized (pageAdminMap){
			pageAdminMap.put(pageKey, pageAdmin);
			pageDataMap.put(pageKey, pageData);
		}
	}



	public static void store(String pageKey, PageAdmin pageAdmin) {
		synchronized (pageAdminMap){
			pageAdminMap.put(pageKey, pageAdmin);
		}
		
	}
	

}
