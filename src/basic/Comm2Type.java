package basic;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

// this is an interface for all the length 2 algorithm
public interface Comm2Type {
    public Map<double[], Set<Integer>> computeComm(String datasetName) throws ExecutionException, InterruptedException;
}
