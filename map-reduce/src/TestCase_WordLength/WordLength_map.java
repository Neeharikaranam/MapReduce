package TestCase_WordLength;
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
import java.util.StringTokenizer;
import java.util.Map.Entry;

import MapperTask.Mapper_function;
import MapperTask.Mapper_obj;

public class WordLength_map extends UnicastRemoteObject implements Mapper_obj, Serializable {
	public WordLength_map() throws RemoteException {
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
		//System.out.print(preprocessed_file);
		return preprocessed_file;
	}
	
	public HashMap<String, List<String>> map(String doc, String input_text) throws RemoteException {
		HashMap<String, List<String>> map = new HashMap<String, List<String>>();
		
		HashMap<String, Integer> intermediate_map = new HashMap<String, Integer>();
		//System.out.println(input_text);
		StringBuilder file = new StringBuilder();
		file = preprocess("Hi", doc);
		String[] words = file.toString().split("\\s+");
		//String[] words = input_text.split("\\s+");
		for (String a : words) {
			int length = a.length();
			String val = String.valueOf(length);
			List<String> value = new ArrayList<String>();
			value.add(val);
			intermediate_map.put(a, length);
		}
		
		System.out.println(intermediate_map);
		
		for (HashMap.Entry<String, Integer> entry : intermediate_map.entrySet()) {
			String key = entry.getKey();
			int val_temp = entry.getValue();
			String val = String.valueOf(val_temp);
			
			if (map.get(val) == null) {
				List<String> value = new ArrayList<String>();
				value.add(key);
				map.put(val, value);
			}
			else {
				List<String> value = new ArrayList<String>();
				value = map.get(val);
				value.add(key);
				map.put(val, value);
				
			}
			
		}
		System.out.println(map);
		return map;
	}
}