package mn.blockdelta.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Collection;
import java.util.Set;


public class MultiSet<K, V> extends HashMap<K, Set<V>> {
	private static final long serialVersionUID = 1L;

	public Map<K, V> pickAnyAttributes (){
		Map<K, V> result = new HashMap<K, V>();
		for (K k:keySet()){
			result.put(k, get(k).iterator().next());
		}
		return result;
	}

	public String toString(){
		StringBuffer result = new StringBuffer();
		for (K k : keySet()){
			result.append(k +" ("+ get(k).size()+"): "+ get(k)+"\r\n");
		}
		return result.toString();
	}



	public void add(K k, V v){
		if (containsKey(k)){
			get(k).add(v);
		} else {
			Set<V> entreeList = new HashSet<V>();
			entreeList.add(v);
			put(k, entreeList);
		}
	}

	public void addAll(Map<K, ? extends Collection<V>> input){
		if (input == null) return;

		for (K k : input.keySet())
			for (V v : input.get(k))
				add(k,v);
	}

	public Map<K, V> toSingleValueMap() {
		Map<K, V> result = new HashMap<K, V>();
		for (K k : keySet()){
			result.put(k, get(k).iterator().next());
		}
		return result;
	}


}