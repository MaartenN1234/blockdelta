package mn.blockdelta.core.conversions.joiner;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;

abstract class AbstractInnerHashTable<K, V> {
	private static final Iterator<Object> emptyIterator = new Iterator<Object>(){
			public boolean hasNext() { return false;}
			public Object  next() 	 { throw new NoSuchElementException();}			
		};
		
	@SuppressWarnings("unchecked")
	protected Iterator<V> emptyIterator() {
		return (Iterator<V>) emptyIterator;
	}		
	protected double getExpectedMatchFactor(){
		return 1d;
	}

	protected abstract Iterator<V> iteratorForJoinMatches(K key, Predicate<V> filter);

	protected Iterator<V> iteratorForNotJoinedRows(){
		throw new UnsupportedOperationException(this.getClass() + "::iteratorForNotJoinedRows not supported.");
	}
	
	
	protected abstract void accept(K key, V value);
	
	protected abstract AbstractInnerHashTable<K, V> mergeWith(AbstractInnerHashTable<K, V> right);

	protected Collector<V, AbstractInnerHashTable<K, V>, AbstractInnerHashTable<K, V>> getCollector(Function <V, K> hashKeyFunction){
		return Collector.<V, AbstractInnerHashTable<K, V>>of(
							()            -> this,
							(map, entry)  -> map.accept(hashKeyFunction.apply((V) entry), entry),
							(left, right) -> left.mergeWith(right));
	}




}