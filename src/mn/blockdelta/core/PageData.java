package mn.blockdelta.core;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import mn.blockdelta.core.conversions.BaseRow;
import mn.blockdelta.core.conversions.RowsourceHeader;

public class PageData {
	public final static PageData EMPTY_PAGE = new PageData(new char[0]);
	
	public char [] data;
	public long    hash;
	
	public PageData(char[] data, long hash){
		this.data = data;
		this.hash = hash;
	}
	public PageData(char[] data){
		this(data, calculateHash(data));
	}	

	

	
	public Stream<BaseRow> streamUsingHeader(RowsourceHeader rowsourceHeader){
		PageDataReader pr = new PageDataReader(this);
		
		Iterator<BaseRow> it = new Iterator<BaseRow>(){
		    public boolean hasNext(){
		    	return pr.hasNext();
		    }
		    public BaseRow next(){
		    	return BaseRow.readFromReader(rowsourceHeader, pr);
		    }
		};
		Spliterator<BaseRow> sp = Spliterators.spliteratorUnknownSize(it, 0);
		return StreamSupport.stream(sp, false);
	}
	
	
	private static long calculateHash(char[] data){
        if (data == null)
            return 0;

        long result = 1;
        for (char element : data)
            result = 31 * result + element;

        return result;
	}
}
