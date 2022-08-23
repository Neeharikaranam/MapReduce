package TestCase_WordCount;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import MapperTask.Mapper_function;
import MapperTask.Mapper_obj;

import java.util.StringTokenizer;

public class WordCounter_map extends UnicastRemoteObject implements Mapper_obj, Serializable {
	public WordCounter_map() throws RemoteException {
		super();
	}
	
	public StringBuilder preprocess(String obj, String splitFileLoc){
		//After getting the intermediate file location,we need to preprocess the file
		StringBuilder preprocessed_file = new StringBuilder();
		try {
			
			FileReader file = new FileReader(splitFileLoc);
			try (BufferedReader br = new BufferedReader(file)) {
				String line = br.readLine();
				String check = new String();
				for (; line != null;) {
					check = line.toLowerCase().replaceAll("[^a-zA-Z0-9]",  " ").trim().replaceAll("\\s+", " ");
					preprocessed_file.append(check);
					preprocessed_file.append("\n");
					line = br.readLine();			 
				}
			
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
		catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return preprocessed_file;
	}
	
	public HashMap<String, List<String>> map(String doc, String random) throws RemoteException {
		HashMap<String, List<String>> map = new HashMap<String, List<String>>();
		
		//System.out.println(input_text);
		StringBuilder file = new StringBuilder();
		file = preprocess("", doc);
		String[] words = file.toString().split("\\s+");
		for (String a : words) {
			if (map.get(a) == null) {
				List<String> value = new ArrayList<String>();
				//System.out.println(a);
				value.add("1");
				map.put(a, value);				
			}
			else {
				List<String> value = new ArrayList<String>();
				value = map.get(a);
				value.add("1");
				map.put(a, value);
			}
		}
		
		System.out.println(map);
		return map;
	}
}