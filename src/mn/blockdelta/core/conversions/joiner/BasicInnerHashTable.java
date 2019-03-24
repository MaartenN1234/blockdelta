package mn.blockdelta.core.conversions.joiner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;



class BasicInnerHashTable<K, V> extends AbstractInnerHashTable<K, V> {
	private int             valueCount;
	private Map<K, List<V>> inner;

	BasicInnerHashTable(){
		inner      = new HashMap<>();
		valueCount = 0;
	}

	@Override
	protected double getExpectedMatchFactor() {			
		return valueCount / ((double) (inner.size()));
	}
	
	@Override
	protected void accept(K key, V value){
		if (!inner.containsKey(key)){
			inner.put(key, new ArrayList<>());				
		} 
		inner.get(key).add(value);
		valueCount++;
	}		
	
	@Override
	protected Iterator<V> iteratorForJoinMatches(K key, Predicate<V> filter) {
		List<V> list = inner.get(key);
		if (list == null){
			return emptyIterator();
		}			
		return new JoinedKeysIterator(list, filter);
	}
	
	
	@Override
	protected BasicInnerHashTable<K, V> mergeWith(AbstractInnerHashTable<K, V> right) {
		if (right instanceof BasicInnerHashTable)
			return mergeWith((BasicInnerHashTable<K,V>) right);
		throw new ClassCastException("Conflicting mergeWith classes.");
	}
	
	private BasicInnerHashTable<K, V> mergeWith(BasicInnerHashTable<K, V> right) {
		for (Entry<K,List<V>> entrees: right.inner.entrySet())
			for (V entry : entrees.getValue())
				accept(entrees.getKey(), entry);
		return this;
	}		
	
	
	private class JoinedKeysIterator implements Iterator<V>{
		int position;
		V   next;
		List<V> list;
		Predicate<V> filter;
		
		private JoinedKeysIterator(List<V> list, Predicate<V> filter){
			this.list     = list;
			this.filter   = filter;
			this.position = 0;
			this.next     = prepNext();
		}

		private V prepNext(){
			while (position < list.size()){
				V result = list.get(position++);
				if (filter.test(result)){
					return result;
				}
			}				
			return null;
		}
		
		@Override
		public boolean hasNext() {
			return next != null;
		}


		@Override
		public V next() {
			if (next == null)
				throw new NoSuchElementException();
			V result = next;
			next     = prepNext();					
			return result;
		}			
	}



}