package mn.blockdelta.test;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.Set;

import mn.blockdelta.core.MasterAdministration;
import mn.blockdelta.core.PageAdmin;
import mn.blockdelta.core.PageData;
import mn.blockdelta.core.PageDataCollector;
import mn.blockdelta.core.conversions.BaseConversion;
import mn.blockdelta.core.conversions.BaseRow;
import mn.blockdelta.core.conversions.RowsourceGenerator;
import mn.blockdelta.core.conversions.RowsourceHeader;
import mn.blockdelta.core.conversions.joiner.JoinStreamProvider;

public class Conv_OUT_EXRATE extends BaseConversion{
	private final static RowsourceHeader HEADER = new RowsourceHeader("OUT_EXRATE",
																new String[]{"CCY_ID","VDATE","XR0","XR1","CCY_RETURN"},
																new int[]{RowsourceHeader.STRING_TYPE,RowsourceHeader.DATETIME_TYPE,
																		RowsourceHeader.DOUBLE_TYPE,RowsourceHeader.DOUBLE_TYPE,RowsourceHeader.DOUBLE_TYPE}
	);

	@Override
	public RowsourceHeader getRowsourceHeader() {
		return HEADER;
	}

	@Override
	public Set<String> getInputPageKeysActual (String[] outputKeys, Map<String, PageData> inputPageData) {
		Set<String> result = new HashSet<String>();
		result.add("EXT_EXRATE"+PageAdmin.PAGE_KEY_SPLIT_CHARACTER+outputKeys[0]);
		
		if (!outputKeys[0].equals(FIRST_DATE)){
			String dateB1 = moveDate(outputKeys[0], -1);
			result.add("OUT_EXRATE"+PageAdmin.PAGE_KEY_SPLIT_CHARACTER+dateB1);			
		}
		return result;
	}

	@Override
	public PageData execute(String[] outputKey, Map<String, PageData> inputPageData) {
		PageData todayInput      = PageData.EMPTY_PAGE;
		PageData yesterdayOutput = PageData.EMPTY_PAGE;
		
		for (Entry<String, PageData> entree : inputPageData.entrySet()){
			if (entree.getKey().startsWith("EXT_EXRATE"))
				todayInput = entree.getValue();
			else 
				yesterdayOutput = entree.getValue();
		}
		RowsourceHeader inputHeader           = MasterAdministration.getRowsourceGenerator("EXT_EXRATE").getRowsourceHeader();
		
		Stream<BaseRow> yesterdayOutputStream = yesterdayOutput.streamUsingHeader(HEADER);
		Stream<BaseRow> todayInputStream      = todayInput.streamUsingHeader(inputHeader);

		PageDataCollector pageDataCollector = new PageDataCollector();
		
		JoinStreamProvider.<String,BaseRow,BaseRow,BaseRow>createJoinStream(
				yesterdayOutputStream, x-> (String) x.get("CCY_ID"), true, 
				todayInputStream,      y-> (String) y.get("FROM_CCY_ID"), true,
				(x,y) -> ((String) x.get("CCY_ID")).equals((String) y.get("FROM_CCY_ID")),
				(x,y) ->   (x == null) ? new BaseRow(HEADER, y.get("FROM_CCY_ID"), y.get("VDATE"), 				y.get("VALUE"), y.get("VALUE"), 1d)
						: ((y == null) ? new BaseRow(HEADER, x.get("CCY_ID"),      moveDate(x.get("VDATE"), 1), x.get("XR1"),   x.get("XR1"),   1d)
					    :                new BaseRow(HEADER, y.get("FROM_CCY_ID"), y.get("VDATE"), 				x.get("XR1"),   y.get("VALUE"), safeDivide(y.get("VALUE"), x.get("XR1"), 1)))
				)
		.forEach(x -> pageDataCollector.collect(x));
		
		return pageDataCollector.toPageData();
	}
}
