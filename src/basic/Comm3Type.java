package basic;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

// this is an interface for all the length 4 algorithm
public interface Comm3Type {
    public Map<double[], Set<Integer>> computeComm(String dataset) throws ExecutionException, InterruptedException;

}
