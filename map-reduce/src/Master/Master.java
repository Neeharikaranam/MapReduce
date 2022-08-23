package Master;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.*;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.ExportException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import MapperTask.Mapper_obj;

public class Master {

    public void master(config conf) throws IOException, InterruptedException, AlreadyBoundException, NotBoundException {
    	
    	//Before starting the process, we need to delete all the intermediate files cached
    	//in before run
    	
    	String mapkey = conf.map_key;
        Utility util = new Utility();
        List<Process> map_process_list = new ArrayList<>();
        String currentDirectory = System.getProperty("user.dir");
        for(String input_filename: conf.master_input_filename){
        	util.partition(input_filename, conf.num_map_processes);
        	String mappertask_loc = "MapperTask/Mapper_function";
        	
        	String Directory = "/Test_files/";
    		int length = currentDirectory.length() + Directory.length();
    		String t = input_filename.substring(length, input_filename.length()-4);
        	for (int i = 0; i < conf.num_map_processes; ++i) {
        		int j = i+1;
        		String mapfile_partition = currentDirectory + "/" + t + "_MapperInput" + j + ".txt";
        		ProcessBuilder p = new ProcessBuilder("java", "-cp", "./bin" , mappertask_loc,
                    config.map_key, input_filename, mapfile_partition, String.valueOf(conf.num_map_processes),
                    String.valueOf(conf.num_reduce_processes),
                    String.valueOf(i + 1));
        		Process mapper = p.inheritIO().start();
        		map_process_list.add(mapper);
        		System.out.println("Process "+i+" is running");
        	}
        }
        
        String temp_file_map = currentDirectory + "/temp_map.txt";
        boolean complete = false;
		while(!complete) {
			File f = new File(temp_file_map);
			if (f.exists()) {
				FileReader fr = new FileReader(temp_file_map);
				@SuppressWarnings("resource")
				BufferedReader br = new BufferedReader(fr);
				int line_count = 0;
				String line = br.readLine();
				while(line != null) {
					line_count++;
					line = br.readLine();	
				}
				if (line_count == (conf.num_map_processes*conf.master_input_filename.size())) {
					complete = true;
				}
				else {
					TimeUnit.MILLISECONDS.sleep(1000);
				}
			}
			else {
				TimeUnit.MILLISECONDS.sleep(1000);
			}
				
		}
		
	    //Kill all the mapper processes
        killprocesses(map_process_list, conf.num_map_processes);
		String reducertask_loc="ReducerTask/Reducer_function";
        List<Process> reduce_process_list = new ArrayList<>();
        String reducer_output_dir = currentDirectory + "/Reducer_output"; //passing the reducer directory
        for (int i = 0; i < conf.num_reduce_processes; i++) {
            String reducer_input_dir = currentDirectory + "/reducer_" + (i+1) ;//passing the reducer directory
        
            String ft;
            if (i != 2) {
            	ft = "false";
            }
            else {
            	ft = "true";
            }
            	
            ProcessBuilder p = new ProcessBuilder("java", "-cp", "./bin", reducertask_loc,
                    config.reduce_key, reducer_input_dir, reducer_output_dir, 
                    Integer.toString(i + 1), ft);
            Process reducer = p.inheritIO().start();
            reduce_process_list.add(reducer);
            System.out.println("Reducer "+i+" is running");

        }
        
        String temp_file_reduce = currentDirectory + "/temp_reduce.txt";
        complete = false;
        int iterations = 0;
        boolean alive = true;
        boolean fault_tol = true;
        // To check the fault tolerance, we are deliberately stopping reducer number 3
		while(!complete) {
	        ++iterations;
			File f = new File(temp_file_reduce);
			if (iterations > 4 && fault_tol) { //here we are assuming the timeout to be maximum of 4 seconds
				reduce_process_list.get(2).destroy();
				alive = false;
				fault_tol = false;
			}
			if (!alive) {
				String reducer_input_dir = currentDirectory + "/reducer_3";
				ProcessBuilder p = new ProcessBuilder("java", "-cp", "./bin", reducertask_loc,
	                    config.reduce_key, reducer_input_dir, reducer_output_dir,
	                    Integer.toString(3), Boolean.toString(alive));
	            Process reducer = p.inheritIO().start();
	            alive = true;
			}
			if (f.exists()) {
				FileReader fr = new FileReader(temp_file_reduce);
				@SuppressWarnings("resource")
				BufferedReader br = new BufferedReader(fr);
				int line_count = 0;
				String line = br.readLine();
				while(line != null) {
					line_count++;
					line = br.readLine();	
				}
				if (line_count == conf.num_reduce_processes) {
					complete = true;
				}
				else {
					TimeUnit.MILLISECONDS.sleep(1000);
				}
			}
			else {
				TimeUnit.MILLISECONDS.sleep(1000);
			}
				
		}

        //Kill all the mapper processes
        killprocesses(reduce_process_list,conf.num_reduce_processes);
        delete_inter_files(currentDirectory, conf.num_reduce_processes, conf.num_map_processes, conf.master_input_filename);
    }  

    public void delete_inter_files(String currentDirectory, int reduce_processes, int map_processes, List<String> inputs) {
    	for(int i=0; i < reduce_processes; i++)
        {
          String rd_ip = currentDirectory + "/reducer_" +(i+1);
          File fp=new File(rd_ip);
          for(File file:fp.listFiles())
          {
             file.delete();
          }
          fp.delete();
        } 
    	String temp_file_name_map = currentDirectory + "/temp_map.txt";
    	File temp_map = new File(temp_file_name_map );
    	temp_map.delete();
    	String temp_file_name_reduce = currentDirectory + "/temp_reduce.txt";
    	File temp_reduce = new File(temp_file_name_reduce );
    	temp_reduce.delete();
    	String Directory = "/Test_files/";
		int length = currentDirectory.length() + Directory.length();
		for(String input_filename: inputs){
			String t = input_filename.substring(length, input_filename.length()-4);
			for(int i = 1; i < map_processes + 1; ++i) {
				String map_input = currentDirectory + "/" + t + "_MapperInput" + i + ".txt";
				File f = new File(map_input);
				f.delete();
			}
		}
    		
    }
    public void killprocesses(List<Process>  Process_list, int num_mappers)
    {
        int i=0;
        while(i<num_mappers)
        {
            Process_list.get(i).destroy();
            i++;
        }
    }
}
