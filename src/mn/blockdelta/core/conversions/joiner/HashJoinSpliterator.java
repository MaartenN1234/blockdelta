package mn.blockdelta.core.conversions.joiner;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

class HashJoinSpliterator<K, V, W, Z> implements Spliterator<Z>{
	private AbstractInnerHashTable<K, V> 	innerHashTable;
	private boolean              		 	outerjoinInnerStreamToDo;
	private Iterator      <V>       		innerStreamIterator;
	private W                       		outerStreamElement;
	private Spliterator   <W>       		outerStreamSpliterator;
	private Function      <W, K>    		hashKeyFunctionOuter;
	private boolean                 		outerjoinOuterStream;
	private BiPredicate   <V, W>    		filter;
	private BiFunction    <V, W, Z> 		projection;
	
	HashJoinSpliterator(AbstractInnerHashTable	<K, V> 	  	innerHashTable,
						boolean              	  			outerjoinInnerStream,
					    Stream         			<W>       	outerStream,
						Function       			<W, K>    	hashKeyFunctionOuter, 
						boolean                  			outerjoinOuterStream,
						BiPredicate    			<V, W>    	filter, 
						BiFunction     			<V, W, Z> 	projection){
		this.innerHashTable           = innerHashTable;
		this.outerjoinInnerStreamToDo = outerjoinInnerStream;
		this.outerStreamSpliterator   = outerStream.spliterator();			
		this.hashKeyFunctionOuter     = hashKeyFunctionOuter;
		this.outerjoinOuterStream     = outerjoinOuterStream;
		this.filter                   = filter;
		this.projection               = projection;
		this.innerStreamIterator      = innerHashTable.emptyIterator();
	}

	@Override
	public boolean tryAdvance(Consumer<? super Z> action) {
		boolean outerStreamSpliteratorWasAdvanced = true;
		
		while (!innerStreamIterator.hasNext() && outerStreamSpliteratorWasAdvanced){
			outerStreamSpliteratorWasAdvanced = outerStreamSpliterator.tryAdvance(w -> outerStreamElement = w);
			if (outerStreamSpliteratorWasAdvanced){
				K joinKey = hashKeyFunctionOuter.apply(outerStreamElement);
				innerStreamIterator = innerHashTable.iteratorForJoinMatches(joinKey, 
																	        v -> filter.test(v, outerStreamElement));
				if (!innerStreamIterator.hasNext() && outerjoinOuterStream){
					action.accept(projection.apply(null, outerStreamElement));
					return true;
				}
			}
		}
		
		if (!innerStreamIterator.hasNext() && outerjoinInnerStreamToDo){
			outerStreamElement       = null;
			outerjoinInnerStreamToDo = false;
			innerStreamIterator      = innerHashTable.iteratorForNotJoinedRows();
		}
		
		if (innerStreamIterator.hasNext()){
			action.accept(projection.apply(innerStreamIterator.next(), outerStreamElement));
			return true;
		}

		return false;
	}

	@Override
	public Spliterator<Z> trySplit() {
		return null;
	}

	@Override
	public long estimateSize() {
		return (long) (outerStreamSpliterator.estimateSize() * 
							((outerjoinOuterStream && innerHashTable.getExpectedMatchFactor() < 1) ? 1 : innerHashTable.getExpectedMatchFactor()))
					;
	}

	@Override
	public int characteristics() {
		return outerStreamSpliterator.characteristics() &
		       (Spliterator.IMMUTABLE | Spliterator.CONCURRENT);
	}
}