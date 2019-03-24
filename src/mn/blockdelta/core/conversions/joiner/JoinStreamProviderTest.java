package mn.blockdelta.core.conversions.joiner;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

public class JoinStreamProviderTest {
	final private List<String> tabOne   = Arrays.asList(new String[]{"A0","A1","A2","B0","B1","C0","D0"});
	final private List<String> tabTwo   = Arrays.asList(new String[]{"A0","A1","C0","D0","D1","E0","E1"});
	final private List<String> tabEmpty = Arrays.asList(new String[]{});
	
	final private List<String> expectedInnerJoinOutput = Arrays.asList(new String[]{"A0,A0","A0,A1","A1,A0","A1,A1","A2,A0","A2,A1","C0,C0","D0,D0","D0,D1"});
	final private List<String> expectedLeftJoinOutput  = Arrays.asList(new String[]{"A0,A0","A0,A1","A1,A0","A1,A1","A2,A0","A2,A1","B0,null","B1,null","C0,C0","D0,D0","D0,D1"});
	final private List<String> expectedRightJoinOutput = Arrays.asList(new String[]{"A0,A0","A0,A1","A1,A0","A1,A1","A2,A0","A2,A1","C0,C0","D0,D0","D0,D1","null,E0","null,E1"});
	final private List<String> expectedOuterJoinOutput = Arrays.asList(new String[]{"A0,A0","A0,A1","A1,A0","A1,A1","A2,A0","A2,A1","B0,null","B1,null","C0,C0","D0,D0","D0,D1","null,E0","null,E1"});
	
	final private List<String> tabOneOuterjoinedNull   = tabOne.stream().map(x->x+",null").collect(Collectors.toList());
	final private List<String> tabTwoOuterjoinedNull   = tabTwo.stream().map(x->"null,"+x).collect(Collectors.toList());
	
	

	@Test
	public final void testCreateHashJoinStreamFullInnerJoin() {
		List<String> testoutput;
		
		testoutput = JoinStreamProvider.<Integer,String,String,String>createJoinStream(
						tabOne.stream(), x->0, false, 
						tabTwo.stream(), y->0, false,
						(x,y) -> x.substring(0, 1).equals(y.substring(0, 1)),
						(x,y) -> x+","+y)
				.sorted()
				.collect(Collectors.toList());
		
		Assert.assertEquals("Inner Join - no join predicate", expectedInnerJoinOutput, testoutput);
		
		testoutput = JoinStreamProvider.<String,String,String,String>createJoinStream(
						tabOne.stream(), x->x.substring(0, 1), false, 
						tabTwo.stream(), y->y.substring(0, 1), false,
						(x,y) -> x.substring(0, 1).equals(y.substring(0, 1)),
						(x,y) -> x+","+y)
				.sorted()
				.collect(Collectors.toList());

		Assert.assertEquals("Inner Join - with join predicate", expectedInnerJoinOutput, testoutput);		
		
		testoutput = JoinStreamProvider.<String,String,String,String>createJoinStream(
						tabTwo.stream(), x->x.substring(0, 1), false, 
						tabOne.stream(), y->y.substring(0, 1), false,
						(x,y) -> x.substring(0, 1).equals(y.substring(0, 1)),
						(x,y) -> y+","+x)
				.sorted()
				.collect(Collectors.toList());

		Assert.assertEquals("Inner Join - with join predicate - flipped inputs", expectedInnerJoinOutput, testoutput);			
	}
	
	@Test
	public final void testCreateHashJoinStreamLeftOuterJoin() {
		List<String> testoutput;
		
		testoutput = JoinStreamProvider.<Integer,String,String,String>createJoinStream(
						tabOne.stream(), x->0, true, 
						tabTwo.stream(), y->0, false,
						(x,y) -> x.substring(0, 1).equals(y.substring(0, 1)),
						(x,y) -> x+","+y)
				.sorted()
				.collect(Collectors.toList());
		
		Assert.assertEquals("Left Join - no join predicate", expectedLeftJoinOutput, testoutput);
		
		testoutput = JoinStreamProvider.<String,String,String,String>createJoinStream(
						tabOne.stream(), x->x.substring(0, 1), true, 
						tabTwo.stream(), y->y.substring(0, 1), false,
						(x,y) -> x.substring(0, 1).equals(y.substring(0, 1)),
						(x,y) -> x+","+y)
				.sorted()
				.collect(Collectors.toList());

		Assert.assertEquals("Left Join - with join predicate", expectedLeftJoinOutput, testoutput);		
		
		testoutput = JoinStreamProvider.<String,String,String,String>createJoinStream(
						tabTwo.stream(), x->x.substring(0, 1), false, 
						tabOne.stream(), y->y.substring(0, 1), true,
						(x,y) -> x.substring(0, 1).equals(y.substring(0, 1)),
						(x,y) -> y+","+x)
				.sorted()
				.collect(Collectors.toList());

		Assert.assertEquals("Left Join - with join predicate - flipped inputs", expectedLeftJoinOutput, testoutput);			
	}

	@Test
	public final void testCreateHashJoinStreamRightOuterJoin() {
		List<String> testoutput;
		
		testoutput = JoinStreamProvider.<Integer,String,String,String>createJoinStream(
						tabOne.stream(), x->0, false, 
						tabTwo.stream(), y->0, true,
						(x,y) -> x.substring(0, 1).equals(y.substring(0, 1)),
						(x,y) -> x+","+y)
				.sorted()
				.collect(Collectors.toList());
		
		Assert.assertEquals("Right Join - no join predicate", expectedRightJoinOutput, testoutput);
		
		testoutput = JoinStreamProvider.<String,String,String,String>createJoinStream(
						tabOne.stream(), x->x.substring(0, 1), false, 
						tabTwo.stream(), y->y.substring(0, 1), true,
						(x,y) -> x.substring(0, 1).equals(y.substring(0, 1)),
						(x,y) -> x+","+y)
				.sorted()
				.collect(Collectors.toList());

		Assert.assertEquals("Right Join - with join predicate", expectedRightJoinOutput, testoutput);		
		
		testoutput = JoinStreamProvider.<String,String,String,String>createJoinStream(
						tabTwo.stream(), x->x.substring(0, 1), true, 
						tabOne.stream(), y->y.substring(0, 1), false,
						(x,y) -> x.substring(0, 1).equals(y.substring(0, 1)),
						(x,y) -> y+","+x)
				.sorted()
				.collect(Collectors.toList());

		Assert.assertEquals("Right Join - with join predicate - flipped inputs", expectedRightJoinOutput, testoutput);			
	}
	@Test
	public final void testCreateHashJoinStreamFullOuterJoin() {
		List<String> testoutput;
		
		testoutput = JoinStreamProvider.<Integer,String,String,String>createJoinStream(
						tabOne.stream(), x->0, true, 
						tabTwo.stream(), y->0, true,
						(x,y) -> x.substring(0, 1).equals(y.substring(0, 1)),
						(x,y) -> x+","+y)
				.sorted()
				.collect(Collectors.toList());
		
		Assert.assertEquals("Outer Join - no join predicate", expectedOuterJoinOutput, testoutput);
		
		testoutput = JoinStreamProvider.<String,String,String,String>createJoinStream(
						tabOne.stream(), x->x.substring(0, 1), true, 
						tabTwo.stream(), y->y.substring(0, 1), true,
						(x,y) -> x.substring(0, 1).equals(y.substring(0, 1)),
						(x,y) -> x+","+y)
				.sorted()
				.collect(Collectors.toList());

		Assert.assertEquals("Outer Join - with join predicate", expectedOuterJoinOutput, testoutput);		
		
		testoutput = JoinStreamProvider.<String,String,String,String>createJoinStream(
						tabTwo.stream(), x->x.substring(0, 1), true, 
						tabOne.stream(), y->y.substring(0, 1), true,
						(x,y) -> x.substring(0, 1).equals(y.substring(0, 1)),
						(x,y) -> y+","+x)
				.sorted()
				.collect(Collectors.toList());

		Assert.assertEquals("Outer Join - with join predicate - flipped inputs", expectedOuterJoinOutput, testoutput);			
	}	
	
	@Test
	public final void testCreateHashJoinStreamEmptySets() {
		List<String> testoutput;
		
		testoutput = JoinStreamProvider.<String,String,String,String>createJoinStream(
						tabEmpty.stream(), x->x.substring(0, 1), false, 
						tabTwo.stream(), y->y.substring(0, 1), false,
						(x,y) -> x.substring(0, 1).equals(y.substring(0, 1)),
						(x,y) -> x+","+y)
				.sorted()
				.collect(Collectors.toList());
		
		Assert.assertEquals("Inner join - Left Empty", tabEmpty, testoutput);
		
		testoutput = JoinStreamProvider.<String,String,String,String>createJoinStream(
						tabOne.stream(), x->x.substring(0, 1), false, 
						tabEmpty.stream(), y->y.substring(0, 1), false,
						(x,y) -> x.substring(0, 1).equals(y.substring(0, 1)),
						(x,y) -> x+","+y)
				.sorted()
				.collect(Collectors.toList());

		Assert.assertEquals("Inner join - Right Empty", tabEmpty, testoutput);	

		testoutput = JoinStreamProvider.<String,String,String,String>createJoinStream(
						tabEmpty.stream(), x->x.substring(0, 1), false, 
						tabEmpty.stream(), y->y.substring(0, 1), false,
						(x,y) -> x.substring(0, 1).equals(y.substring(0, 1)),
						(x,y) -> x+","+y)
				.sorted()
				.collect(Collectors.toList());
		
		Assert.assertEquals("Inner join - Both Empty", tabEmpty, testoutput);	
		
		
		
		testoutput = JoinStreamProvider.<String,String,String,String>createJoinStream(
						tabEmpty.stream(), x->x.substring(0, 1), true, 
						tabTwo.stream(), y->y.substring(0, 1), false,
						(x,y) -> x.substring(0, 1).equals(y.substring(0, 1)),
						(x,y) -> x+","+y)
				.sorted()
				.collect(Collectors.toList());
		
		Assert.assertEquals("Left join - Left Empty", tabEmpty, testoutput);
		

		testoutput = JoinStreamProvider.<String,String,String,String>createJoinStream(
						tabOne.stream(), x->x.substring(0, 1), true, 
						tabEmpty.stream(), y->y.substring(0, 1), false,
						(x,y) -> x.substring(0, 1).equals(y.substring(0, 1)),
						(x,y) -> x+","+y)
				.sorted()
				.collect(Collectors.toList());
		
		Assert.assertEquals("Left join - Right Empty", tabOneOuterjoinedNull, testoutput);	

		testoutput = JoinStreamProvider.<String,String,String,String>createJoinStream(
						tabEmpty.stream(), x->x.substring(0, 1), true, 
						tabEmpty.stream(), y->y.substring(0, 1), false,
						(x,y) -> x.substring(0, 1).equals(y.substring(0, 1)),
						(x,y) -> x+","+y)
				.sorted()
				.collect(Collectors.toList());

		Assert.assertEquals("Left join - Both Empty", tabEmpty, testoutput);			

	
		
		testoutput = JoinStreamProvider.<String,String,String,String>createJoinStream(
						tabEmpty.stream(), x->x.substring(0, 1), false, 
						tabTwo.stream(), y->y.substring(0, 1), true,
						(x,y) -> x.substring(0, 1).equals(y.substring(0, 1)),
						(x,y) -> x+","+y)
				.sorted()
				.collect(Collectors.toList());
		
		Assert.assertEquals("Right join - Left Empty", tabTwoOuterjoinedNull, testoutput);
		
		testoutput = JoinStreamProvider.<String,String,String,String>createJoinStream(
						tabOne.stream(), x->x.substring(0, 1), false, 
						tabEmpty.stream(), y->y.substring(0, 1), true,
						(x,y) -> x.substring(0, 1).equals(y.substring(0, 1)),
						(x,y) -> x+","+y)
				.sorted()
				.collect(Collectors.toList());
		
		Assert.assertEquals("Right join - Right Empty", tabEmpty, testoutput);	
		
		
		testoutput = JoinStreamProvider.<String,String,String,String>createJoinStream(
						tabEmpty.stream(), x->x.substring(0, 1), false, 
						tabEmpty.stream(), y->y.substring(0, 1), true,
						(x,y) -> x.substring(0, 1).equals(y.substring(0, 1)),
						(x,y) -> x+","+y)
				.sorted()
				.collect(Collectors.toList());
		
		Assert.assertEquals("Right join - Both Empty", tabEmpty, testoutput);		
	
		
		testoutput = JoinStreamProvider.<String,String,String,String>createJoinStream(
						tabEmpty.stream(), x->x.substring(0, 1), true, 
						tabTwo.stream(), y->y.substring(0, 1), true,
						(x,y) -> x.substring(0, 1).equals(y.substring(0, 1)),
						(x,y) -> x+","+y)
				.sorted()
				.collect(Collectors.toList());
		
		Assert.assertEquals("Outer join - Left Empty", tabTwoOuterjoinedNull, testoutput);		
		
		testoutput = JoinStreamProvider.<String,String,String,String>createJoinStream(
						tabOne.stream(), x->x.substring(0, 1), true, 
						tabEmpty.stream(), y->y.substring(0, 1), true,
						(x,y) -> x.substring(0, 1).equals(y.substring(0, 1)),
						(x,y) -> x+","+y)
				.sorted()
				.collect(Collectors.toList());
		
		Assert.assertEquals("Outer join - Right Empty", tabOneOuterjoinedNull, testoutput);	

		testoutput = JoinStreamProvider.<String,String,String,String>createJoinStream(
						tabEmpty.stream(), x->x.substring(0, 1), true,
						tabEmpty.stream(), y->y.substring(0, 1), true,
						(x,y) -> x.substring(0, 1).equals(y.substring(0, 1)),
						(x,y) -> x+","+y)
				.sorted()
				.collect(Collectors.toList());

		Assert.assertEquals("Outer join - Both Empty", tabEmpty, testoutput);			
	}	
}
