package TestCase_WordCount;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import ReducerTask.Reducer_obj;

public class WordCounter_reduce extends Reducer_obj{
	
	public List<String> reduce(String key, List<String> value) {
		
		List<String> count = new ArrayList<String>();
		
		int sum = 0;
		
		for (String i : value) {
			
			sum += Integer.parseInt(i);
			
		}
		count.add(Integer.toString(sum));
		
		return count;
	}
}
