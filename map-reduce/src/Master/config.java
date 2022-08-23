package Master;

import java.util.List;

import MapperTask.Mapper_obj;
import ReducerTask.Reducer_obj;

public class config {
	//We'll first declare mapper_obj, the map function of the udf we want to use
	//Mapper obj to be used
    public Mapper_obj mapper;
    // Map function name specific to UDF
    public static String map_key;
    // Number of mappers to be started
    public int num_map_processes;
    //Input file name for the master
    public List<String> master_input_filename;
    //Reducer object to be used
    public Reducer_obj reducer;
    //Reduce function name specific to UDF
    public static String  reduce_key;
    // Number of reducers to be started
    public int num_reduce_processes;
 // folder of the reducer's output files
    public String reducer_output_folder;
}
