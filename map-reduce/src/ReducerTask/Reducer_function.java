package ReducerTask;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import TestCase_SearchWord.SearchWord_reduce;
import TestCase_WordCount.WordCounter_reduce;
import TestCase_WordLength.WordLength_reduce;

public class Reducer_function {
	

	public HashMap<String, List<String>> readingFile(String reduce_directory) {
		
		File files = new File(reduce_directory);
		//System.out.println(files);
		HashMap<String, List<String>> reduceOutput =  new HashMap<>();
		
		for (File file : files.listFiles()) {
			
			String fileName = file.getAbsolutePath();
			try (Stream<String> sentences = Files.lines(Paths.get(fileName))) {
				
				sentences.forEach(sentence -> {
					StringTokenizer st = new StringTokenizer(sentence, ":");
					while (st.hasMoreTokens()) {
						
						String key = new String(st.nextToken());
						if (reduceOutput.get(key)==null) {
							String values =  st.nextToken();
							StringTokenizer valuesSplit = new StringTokenizer(values, "[, ]");
							List valuesList = new ArrayList();
							while(valuesSplit.hasMoreTokens()) {
								valuesList.add(valuesSplit.nextToken());
							}
							reduceOutput.put(key, valuesList);
						}
						else {
							boolean keyPresent = reduceOutput.containsKey(key);
							if (keyPresent) {
								String values =  st.nextToken();
								StringTokenizer valuesSplit = new StringTokenizer(values, "[, ]");
								List valuesList = new ArrayList();
								List x = reduceOutput.get(key);
								while(valuesSplit.hasMoreTokens()) {
									valuesList.add(valuesSplit.nextToken());
								}
							valuesList.addAll(x);
							reduceOutput.put(key, valuesList);
							}
							else {
								String values =  st.nextToken();
								StringTokenizer valuesSplit = new StringTokenizer(values, "[, ]");
								List valuesList = new ArrayList();
								while(valuesSplit.hasMoreTokens()) {
									valuesList.add(valuesSplit.nextToken());
								}
							reduceOutput.put(key, valuesList);
							}
						}
					}
				});
			} catch (IOException e) {
			
				System.out.println("Error has occured");
			}
		}
		return reduceOutput;
	}
	
	
	public void reduce(HashMap<String, List<String>> reduceOutput,String reducerOutputFile, Reducer_obj rf, boolean ft, int process_num) throws InterruptedException{//Reducer_function rf) {
		
		File reduceOutputFile = new File(reducerOutputFile);
		String currentDirectory = System.getProperty("user.dir");
		String temp_file_name = currentDirectory + "/temp_reduce.txt";
		try {
			BufferedWriter bWriter = new BufferedWriter(new FileWriter (reduceOutputFile));
			for(Entry<String, List<String>> entry : reduceOutput.entrySet()){
				if (ft) {
					System.out.println("program is running");
					TimeUnit.MILLISECONDS.sleep(5000);
				}
				
				List output = new ArrayList<>();
				output = rf.reduce(entry.getKey(), entry.getValue());
				
				bWriter.write("<" +entry.getKey() +" : " +output.get(0)+ ">");
			
				bWriter.newLine();
		        bWriter.flush();
		       	
			}
		bWriter.close();
		System.out.println("Program is completed");
		File temp = new File(temp_file_name);
        FileWriter fw = new FileWriter(temp_file_name, true);
        FileReader fr = new FileReader(temp_file_name);
        try (FileChannel channel = new RandomAccessFile(temp, "rw").getChannel()) {
			BufferedReader br = new BufferedReader(fr);
			BufferedWriter bw2 = new BufferedWriter(fw);
			FileLock lock = channel.lock();
			String line = br.readLine();
			if (line != null) {
				bw2.write(process_num + " is completed!");
				bw2.newLine();
				//bw2.write(new_line);
				bw2.close();
			}
			else {
				bw2.write(process_num + " is completed!");
				bw2.newLine();
				bw2.close();
			}  
			//TimeUnit.MINUTES.sleep(1);
			lock.release();
		}
		} catch (IOException e) {
			
			System.out.println("Error has occured.");
		}
	}
	
	public static void main(String args[]) throws IOException, InterruptedException{
		
		String reduce_key = args[0];
		String reduce_Directory = args[1];
		String reduce_Output_Directory = args[2];
		int process_num = Integer.parseInt(args[3]);
		String fault_tolerance_boolean = args[4];
		System.out.println("In reducer");
		boolean ft = Boolean.parseBoolean(fault_tolerance_boolean);
		
		HashMap<String, List<String>> reduceOutput =  new HashMap<>();
		
		Reducer_function rt = new Reducer_function();
		reduceOutput = rt.readingFile(reduce_Directory);
		
		String reducerOutputFile = null;
		
		Reducer_obj rf = null;

		  if (reduce_key.equals("WordCounter_reduce")){
			  rf = new WordCounter_reduce();
			  System.out.println(reduceOutput);
			  String reducer_output_folder = reduce_Output_Directory +"/reducer_output_WC";
			  File wc_directory = new File(reducer_output_folder);
	          wc_directory.mkdir();
			  reducerOutputFile = reduce_Output_Directory + "/reducer_output_WC/reducer_" + process_num + ".txt";
		  }
		  else if (reduce_key.equals("SearchWord_reduce")) {
			  rf = new SearchWord_reduce();
			  System.out.println(reduceOutput);
			  String reducer_output_folder = reduce_Output_Directory +"/reducer_output_SW";
			  File wc_directory = new File(reducer_output_folder);
	          wc_directory.mkdir();
			  reducerOutputFile = reduce_Output_Directory + "/reducer_output_SW/reducer_" + process_num + ".txt";
		  }
		  else {
			  rf = new WordLength_reduce();
			  String reducer_output_folder = reduce_Output_Directory +"/reducer_output_WL";
			  File wc_directory = new File(reducer_output_folder);
	          wc_directory.mkdir();
			  reducerOutputFile = reduce_Output_Directory + "/reducer_output_WL/reducer_" + process_num + ".txt";
		  }
		
		rt.reduce(reduceOutput, reducerOutputFile, rf, ft, process_num);
		
	}

}
