package TestCase_WordCount;
import java.io.File;
import java.io.IOException;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;

import Master.Master;
import Master.config;
import java.io.*;
import java.nio.channels.*;

public class WordCounter {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, AlreadyBoundException, NotBoundException
    {
        try {
            // specifying all input parameters
            config c = new config();
            c.num_map_processes = 4;
            String currentDirectory = System.getProperty("user.dir");
            
			String master_input_filename = currentDirectory + "/Test_files/Test.txt";
            c.master_input_filename = new ArrayList<String>();
            c.master_input_filename.add(master_input_filename);
            config.map_key = "WordCounter_map";
            c.mapper = new WordCounter_map();
            config.reduce_key = "WordCounter_reduce";
            c.reducer = new WordCounter_reduce();
            String reducer_output_folder = currentDirectory +"/Reducer_output";
            c.reducer_output_folder = new String(reducer_output_folder);
            // Create the directory during the start
            File wc_directory = new File(reducer_output_folder);
            wc_directory.mkdir();
            c.num_reduce_processes = 4;
            //Create a temp file
            String temp_file_name_map = currentDirectory + "/temp_map.txt";
            String temp_file_name_reduce = currentDirectory + "/temp_reduce.txt";
            File temp = new File(temp_file_name_map);
            File temp1 = new File(temp_file_name_reduce);
            Master obj = new Master();
            // call map reduce library
            obj.master(c);
            System.exit(0);             // required since mapper and reducer extend UnicastRemoteObject
        }
        catch (RemoteException e) {
           e.printStackTrace();
        }
    }
}