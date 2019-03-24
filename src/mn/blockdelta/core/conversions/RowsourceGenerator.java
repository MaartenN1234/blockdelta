package mn.blockdelta.core.conversions;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import mn.blockdelta.core.PageAdmin;
import mn.blockdelta.core.PageData;

public interface RowsourceGenerator {
	public static Set<String> EMPTY_INPUT_SET   = new HashSet<String>();
	public static String []   EMPTY_OUTPUT_KEYS = new String[0];
	
	public RowsourceHeader    getRowsourceHeader();
	public boolean            usesExternalInput();


	default public Set<String> getInputPageKeysConfig (@SuppressWarnings("unused") String[] outputKeys)  {
		return EMPTY_INPUT_SET;
	}; 
	public Set<String> getInputPageKeysActual (String[] outputKeys, Map<String, PageData> inputPageData); 
	public PageData    execute                (String[] outputKeys, Map<String, PageData> inputPageData); 
}
