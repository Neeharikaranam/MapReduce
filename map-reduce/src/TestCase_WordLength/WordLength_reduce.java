package TestCase_WordLength;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import ReducerTask.Reducer_obj;

public class WordLength_reduce extends Reducer_obj{
public List<String> reduce(String key, List<String> value) {
		
		List<String> count = new ArrayList<String>();
		Set<String> removeDuplicates = new LinkedHashSet<String>();
		
		removeDuplicates.addAll(value);
		value.clear();
		value.addAll(removeDuplicates);
		System.out.print(key + "=" + value );
		
		int sum = 0;
		
		for (String i : value) {
			
			sum += 1;
			
		}
		count.add(Integer.toString(sum));
		
		return count;
	}
	
}
