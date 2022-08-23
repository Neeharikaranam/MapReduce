package Master;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.rmi.*;
import java.time.Instant;

public class Utility {

    public void partition(String input, int num_mapreduce) throws IOException {
        FileInputStream fs = new FileInputStream(input);
        DataInputStream ds = new DataInputStream((fs));
        BufferedReader br = new BufferedReader(new InputStreamReader(ds));
        String writeline;//line to be written

        /*Count the number of lines */
        int number = 0;
        BufferedReader br_1 = new BufferedReader(new FileReader(input));
        String line = br_1.readLine();
        while (line != null) {
            line = br_1.readLine();
            number++;
        }
        br_1.close();

        /*Split the lines into files */

        String currentDirectory = System.getProperty("user.dir");
		String Directory = "/Test_files/";
		int length = currentDirectory.length() + Directory.length();
		String t = input.substring(length, input.length()-4);
		
		int main_lines = number/4;
	    int extra_lines = number %4;
	    List<Integer> num_lines = new ArrayList<Integer>();
	    for(int i = 0 ; i < extra_lines; ++i){
            num_lines.add(main_lines + 1);
        }
        for (int i = 0; i < num_mapreduce - extra_lines; ++i){
            num_lines.add(main_lines);
        }
        String line_infile;
        int i = 1;	
        for (int temp: num_lines) {
          FileWriter fwrite = new FileWriter(t + "_MapperInput" + i + ".txt");
          BufferedWriter bw = new BufferedWriter(fwrite);
          for( int j = 1; j <= temp; j++) {
        		line_infile = br.readLine();
        		if(line_infile != null) {
        			bw.write(line_infile);
        		}
        		if (j != temp) {
                       bw.newLine();
                }
        	}
        	bw.close();
        	++i;
        }
        ds.close();
    }


}


