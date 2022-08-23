package MapperTask;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.List;

public interface Mapper_obj extends Remote{
	HashMap<String, List<String>> map(String k, String v) throws RemoteException;
}
