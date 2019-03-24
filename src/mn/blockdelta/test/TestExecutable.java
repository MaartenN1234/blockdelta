package mn.blockdelta.test;

import java.util.Arrays;

import mn.blockdelta.core.MasterAdministration;
import mn.blockdelta.core.PageAdmin;
import mn.blockdelta.core.PageData;
import mn.blockdelta.core.PageDataCollector;
import mn.blockdelta.core.conversions.SQLTableLoader;
import mn.blockdelta.core.conversions.RowsourceGenerator;
import mn.blockdelta.core.conversions.RowsourceHeader;

public class TestExecutable {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		registerRowsourceGenerators();
		
		long t = System.currentTimeMillis();
		
		PageAdmin displayPage = new PageAdmin("OUT_EXRATE","20180202");
		displayPage.ensurePageIsSynced(1, System.currentTimeMillis());
		displayPage.dumpCurrentContents();
		
		System.out.println("Time spend "+ (System.currentTimeMillis()-t)+"ms");
		
	}
	private static void registerRowsourceGenerators(){
		MasterAdministration.registerRowsourceGenerator(
				"EXT_EXRATE", new SQLTableLoader(x -> x.length > 0 ? "SELECT * FROM EXT_EXRATE WHERE FXSET_ID ='D' AND VDATE = TO_DATE('"+x[0]+"','YYYYMMDD')" :
																		"SELECT * FROM EXT_EXRATE WHERE FXSET_ID ='D'"));
		MasterAdministration.registerRowsourceGenerator(
				"OUT_EXRATE", new Conv_OUT_EXRATE());
		
	}

}
