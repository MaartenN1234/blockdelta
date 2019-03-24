package mn.blockdelta.core.conversions.joiner;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class JoinStreamProviderPerformanceTest {
	final static int bigTableCount     = 2500000;
	final static int smallTableCount   = 20000;
	final static int tinyTableCount    = 2000;
	final static int keyJoinedKeyCount = 10000;
	
    @Before 
    public void before()  {
    	System.gc();	
    	try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
		}
    }

	
	@Test
	public final void performanceTestInnerJoinNonKey(){
		Stream<Pair> bigTabOne = createBigTableStream(50000, 1000);
		Stream<Pair> bigTabTwo = createBigTableStream(800,  1000);

		long snapshot = System.currentTimeMillis();		
		long testOutput = JoinStreamProvider.<Integer,Pair,Pair,Pair>createJoinStream(
						bigTabOne, x->0, false, 
						bigTabTwo, y->0, false,
						(x,y) -> x.k==y.k,
						(x,y) -> x)
				.count();	

		System.out.println("Non key join of " +testOutput+ " rows in " + (System.currentTimeMillis() -  snapshot) +"ms");
	}
	
	@Test
	public final void performanceTestInnerJoinWithKeyTiny(){
		Stream<Pair> bigTabOne = createBigTableStream(bigTableCount*4, keyJoinedKeyCount);
		Stream<Pair> bigTabTwo = createBigTableStream(tinyTableCount,  keyJoinedKeyCount);
		
		long snapshot = System.currentTimeMillis();		
		long testOutput = JoinStreamProvider.<Integer,Pair,Pair,Pair>createJoinStream(
						bigTabOne, x->x.k, false, 
						bigTabTwo, y->y.k, false,
						(x,y) -> x.k==y.k,
						(x,y) -> x)
				.count();	

		System.out.println("Key join with tiny of " +testOutput+ " rows in " + (System.currentTimeMillis() -  snapshot) +"ms");
	}	
	
	@Test
	public final void performanceTestInnerJoinWithKey(){
		Stream<Pair> bigTabOne = createBigTableStream(bigTableCount,   keyJoinedKeyCount);
		Stream<Pair> bigTabTwo = createBigTableStream(smallTableCount, keyJoinedKeyCount);
		
		long snapshot = System.currentTimeMillis();		
		long testOutput = JoinStreamProvider.<Integer,Pair,Pair,Pair>createJoinStream(
						bigTabOne, x->x.k, false, 
						bigTabTwo, y->y.k, false,
						(x,y) -> x.k==y.k,
						(x,y) -> x)
				.count();	

		System.out.println("Key join of " +testOutput+ " rows in " + (System.currentTimeMillis() -  snapshot) +"ms");
	}	
	
	
	@Test
	public final void performanceTestInnerJoinWithKey2(){
		Stream<Pair> bigTabOne = createBigTableStream(bigTableCount,   keyJoinedKeyCount);
		Stream<Pair> bigTabTwo = createBigTableStream(smallTableCount, keyJoinedKeyCount);
		
		long snapshot = System.currentTimeMillis();		
		long testOutput = JoinStreamProvider.<Integer,Pair,Pair,Pair>createJoinStream(
						bigTabOne, x->x.k, false, 
						bigTabTwo, y->y.k, false,
						(x,y) -> x.k==y.k,
						(x,y) -> x)
				.count();	

		System.out.println("Key join of " +testOutput+ " rows in " + (System.currentTimeMillis() -  snapshot) +"ms");
	}		
	
	@Test
	public final void performanceTestInnerJoinWithKeyFlipped(){
		Stream<Pair> bigTabOne = createBigTableStream(bigTableCount,   keyJoinedKeyCount);
		Stream<Pair> bigTabTwo = createBigTableStream(smallTableCount, keyJoinedKeyCount);

		long snapshot = System.currentTimeMillis();		
		long testOutput = JoinStreamProvider.<Integer,Pair,Pair,Pair>createJoinStream(
					bigTabOne, x->x.k, false, 
					bigTabTwo, y->y.k, false,
					(x,y) -> x.k==y.k,
					(x,y) -> x)
				.count();	

		System.out.println("Key join (flipped inputs) of " +testOutput+ " rows in " + (System.currentTimeMillis() -  snapshot) +"ms");
	}	
	
	@Test
	public final void performanceTestOuterJoinWithKey(){
		Stream<Pair> bigTabOne = createBigTableStream(bigTableCount,   keyJoinedKeyCount);
		Stream<Pair> bigTabTwo = createBigTableStream(smallTableCount, keyJoinedKeyCount);
	
		long snapshot = System.currentTimeMillis();		
		long testOutput = JoinStreamProvider.<Integer,Pair,Pair,Pair>createJoinStream(
						bigTabOne, x->x.k, true, 
						bigTabTwo, y->y.k, true,
						(x,y) -> x.k==y.k,
						(x,y) -> x)
				.count();	

		System.out.println("Key outerjoin of " +testOutput+ " rows in " + (System.currentTimeMillis() -  snapshot) +"ms");
	}	
	
	@Test
	public final void performanceTestOuterJoinWithKeyFlipped(){
		Stream<Pair> bigTabOne = createBigTableStream(bigTableCount,   keyJoinedKeyCount);
		Stream<Pair> bigTabTwo = createBigTableStream(smallTableCount, keyJoinedKeyCount);

		long snapshot = System.currentTimeMillis();		
		long testOutput = JoinStreamProvider.<Integer,Pair,Pair,Pair>createJoinStream(
						bigTabOne, x->x.k, true, 
						bigTabTwo, y->y.k, true,
						(x,y) -> x.k==y.k,
						(x,y) -> x)
				.count();	

		System.out.println("Key outerjoin (flipped inputs) of " +testOutput+ " rows in " + (System.currentTimeMillis() -  snapshot) +"ms");
	}		
	

	
	
	private static class Pair{
		int k;
		@SuppressWarnings("unused")
		int v;
		Pair(int k, int v){
			this.k = k;
			this.v = v;
		}
	}	
	private static Stream<Pair> createBigTableStream(int elements, int possibleDistinctKeys){
		Spliterator<Pair> spliterator = new Spliterator<Pair>(){
			int i=0;
			int seed = (((elements << 16) * 31) << 16) * 31;

			@Override
			public boolean tryAdvance(Consumer<? super Pair> action) {
				if (i < elements){
					int key = (seed + i<<16) % possibleDistinctKeys;
					action.accept(new Pair(key, i));
				}
				return i++ < elements;
			}

			@Override
			public Spliterator<Pair> trySplit() {
				return null;
			}

			@Override
			public long estimateSize() {
				return (elements-i);
			}

			@Override
			public int characteristics() {
				return Spliterator.SIZED | Spliterator.DISTINCT | Spliterator.IMMUTABLE | Spliterator.ORDERED | Spliterator.NONNULL;
			}
		
		};

		return StreamSupport.stream(spliterator,false);
	}	
	

}
