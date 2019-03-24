package mn.blockdelta.core.conversions.joiner;

import java.util.Spliterators;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class JoinStreamProvider{	
	private static <K, V> AbstractInnerHashTable<K, V> createHashJoinInnerTable(
			Stream      <V>       innerStream,   
			Function    <V, K>    hashKeyFunctionInner,
			boolean               outerjoinInnerStream){
		
		AbstractInnerHashTable<K, V> resultImplementationType =
				outerjoinInnerStream ?
						new InnerHashTableWithOuterJoinSupport<K,V>() :
						new BasicInnerHashTable<K,V>();

		return	innerStream
					.collect(resultImplementationType.getCollector(hashKeyFunctionInner));	

	}	

	public static <K, V, W, Z> Stream<Z> createJoinStream(
			Stream      <V>       innerStream,   
			Function    <V, K>    hashKeyFunctionInner,
			boolean               outerjoinInnerStream,
			Stream      <W>       outerStream,
			Function    <W, K>    hashKeyFunctionOuter,
			boolean               outerjoinOuterStream,
			BiPredicate <V, W>    filter, 
			BiFunction  <V, W, Z> projection){
		AbstractInnerHashTable<K, V>    innerHashTable = createHashJoinInnerTable(	innerStream, 
																					hashKeyFunctionInner,
																					outerjoinInnerStream);
		HashJoinSpliterator<K, V, W, Z> spliterator    = new HashJoinSpliterator<K, V, W, Z>(	innerHashTable,
																								outerjoinInnerStream,
																								outerStream,
																								hashKeyFunctionOuter,
																								outerjoinOuterStream,
																								filter,
																								projection);		
		return StreamSupport.stream(spliterator, false);
	}
}
