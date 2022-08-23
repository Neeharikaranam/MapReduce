package MapperTask;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.net.MalformedURLException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import Master.Master;
import Master.config;
import TestCase_SearchWord.SearchWord_map;
import TestCase_WordCount.WordCounter_map;
import TestCase_WordLength.WordLength;
import TestCase_WordLength.WordLength_map;

public class Mapper_function {
	//Mapper
	//Now that we have preprocessed information, we need to create write intermediate file for each mapper
	public void mapper(Mapper_obj obj, String input_fileLoc, String split_fileLoc, int num_map_processes, int num_reduce_processes, int process_num) throws IOException, InterruptedException {
		//Mapper_obj
		HashMap<String, List<String>> mapper_output1 = obj.map(split_fileLoc, input_fileLoc);
		System.out.println(mapper_output1);
		String currentDirectory = System.getProperty("user.dir");
		List<BufferedWriter> bufferedWriters = new ArrayList<BufferedWriter>();
		long epoch = Instant.now().getEpochSecond();
		String Directory = "/Test_files/";
		int length = currentDirectory.length() + Directory.length();
		String t = input_fileLoc.substring(length, input_fileLoc.length()-4);
		String tempFileName = epoch + "_" + t +"_intermediate_process_" + process_num + ".txt";
		String temp_file_name = currentDirectory + "/temp_map.txt";
		
		//System.out.print(tempFileName);
		int num_processes = num_map_processes;
		int i = 0;
		while(i < num_reduce_processes) {
			++i;
			
			String new_reduce_directory = currentDirectory + "/reducer_" + i;
			//System.out.println(new_reduce_directory);
			File directory = new File(new_reduce_directory);
			if (!directory.exists()) {
				directory.mkdir();
			}
			String fileName = new String();
			fileName = new_reduce_directory + "/" + tempFileName;
			System.out.println(tempFileName);
			if (!directory.exists()) directory.mkdir();
			File tempFile = new File(fileName);
			//System.out.println(tempFile);
            if (!tempFile.exists()) {
            	tempFile.createNewFile();
            }
            
            FileWriter f_writer = new FileWriter(tempFile.getAbsoluteFile());
            System.out.println(tempFile.getAbsoluteFile());
            BufferedWriter b_writer = new BufferedWriter(f_writer);
            bufferedWriters.add(b_writer);
		}
		
		for (HashMap.Entry<String, List<String>> entry : mapper_output1.entrySet()) {
			String key = entry.getKey();
			List<String> values = new ArrayList<String>();
			values = entry.getValue();
			int hash_value = Math.abs(key.hashCode()) % num_reduce_processes;
			BufferedWriter bw = bufferedWriters.get(hash_value);
			bw.write(key + ":" + values);
			bw.newLine();
		}
		for (BufferedWriter bufferedWriter : bufferedWriters) {
            bufferedWriter.close();
		}
		//Let us maintain a temp file
		//Whenever the mapper completed writing to the respective intermediate reducer files
		// add the process num to the temp file.
		File temp = new File(temp_file_name);
        FileWriter fw = new FileWriter(temp_file_name, true);
        FileReader fr = new FileReader(temp_file_name);
        FileChannel channel = new RandomAccessFile(temp, "rw").getChannel();
        BufferedReader br = new BufferedReader(fr);
        BufferedWriter bw2 = new BufferedWriter(fw);
        FileLock lock = channel.lock();
        String line = br.readLine();
        if (line != null) {
        	bw2.write(process_num + " is completed!");
        	bw2.newLine();
        	bw2.close();
        }
        else {
        	int j = 1;
        	bw2.write(process_num + " is completed!");
        	bw2.newLine();
        	bw2.close();
        }  
        lock.release();
	}
	 
	 public static void main(String[] args) throws NumberFormatException, IOException, NotBoundException, InterruptedException {
		  //When process Builder is called with the parameters, we need to read the values
		  String map_key = args[0];
		  String main_input_filename = args[1];
		  String partition_filename = args[2];
		  String num_map_process = args[3];
		  String num_reduce_process = args[4];
		  String proc_num = args[5];
		  Mapper_obj map_obj = null;
		  if (map_key.equals("WordCounter_map")){
			  map_obj = new WordCounter_map();
		  }
		  else if (map_key.equals("SearchWord_map")) {
			  map_obj = new SearchWord_map();
		  }
		  else {
			  map_obj = new WordLength_map();
		  }
		  // Declare and initialize mapper_function and call the mapper
		  Mapper_function map_func = new Mapper_function();
		  map_func.mapper(map_obj, main_input_filename, partition_filename, Integer.parseInt(num_map_process),  
				  Integer.parseInt(num_reduce_process), Integer.parseInt(proc_num));
	 }
}





























