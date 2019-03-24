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


class InnerHashTableWithOuterJoinSupport<K, V> extends AbstractInnerHashTable<K, V> {
	private int                      valueCount;
	private Map<K, List<InnerEntry>> inner;

	InnerHashTableWithOuterJoinSupport(){
		inner      = new HashMap<K, List<InnerEntry>>();
		valueCount = 0;
	}

	@Override
	protected double getExpectedMatchFactor() {			
		return valueCount / ((double) (inner.size()));
	}

	@Override
	protected void accept(K key, V value){
		put(key, new InnerEntry(value));
	}
	
	private void put(K key, InnerEntry innerEntry){
		if (!inner.containsKey(key)){
			inner.put(key, new ArrayList<InnerEntry>());				
		} 
		inner.get(key).add(innerEntry);
		valueCount++;
	}		
	
	@Override
	protected Iterator<V> iteratorForJoinMatches(K key, Predicate<V> filter) {
		List<InnerEntry> list = inner.get(key);
		if (list == null){
			return emptyIterator();
		}	
		
		return new JoinedKeysIterator(list, filter);
	}
	@Override
	protected Iterator<V> iteratorForNotJoinedRows(){
		return inner.values()
				.stream()
				.flatMap(x -> x.stream())
				.filter(x -> !x.wasJoined)
				.map(x -> x.value)
				.iterator();
	}

	@Override
	protected InnerHashTableWithOuterJoinSupport<K, V> mergeWith(AbstractInnerHashTable<K, V> right) {
		if (right instanceof InnerHashTableWithOuterJoinSupport)
			return mergeWith((InnerHashTableWithOuterJoinSupport<K,V>) right);
		throw new ClassCastException("Conflicting mergeWith classes.");
	}
	
	private InnerHashTableWithOuterJoinSupport<K, V> mergeWith(InnerHashTableWithOuterJoinSupport<K, V> right) {
		for (Entry<K,List<InnerEntry>> entrees: right.inner.entrySet())
			for (InnerEntry entry : entrees.getValue())
				put(entrees.getKey(), entry);
		return this;
	}		
	
	
	private class JoinedKeysIterator implements Iterator<V>{
		int position;
		V   next;
		List<InnerEntry> list;
		Predicate<V> filter;
		
		private JoinedKeysIterator(List<InnerEntry> list, Predicate<V> filter){
			this.list     = list;
			this.filter   = filter;
			this.position = 0;
			this.next     = prepNext();
		}

		private V prepNext(){
			while (position < list.size()){
				InnerEntry result = list.get(position++);
				if (filter.test(result.value)){
					result.wasJoined = true;
					return result.value;
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

	private class InnerEntry{
		private boolean wasJoined = false;
		private V       value;
		private InnerEntry(V value){
			this.value = value;
		}
		public String toString(){
			return value.toString();
		}
	}
}