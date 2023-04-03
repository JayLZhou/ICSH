package build;
import java.util.HashSet;
import java.util.Set;

import util.Config;
import util.DataReader;
import util.MetaPath;

public class HomoGraphBuilder {
	private int graph[][] = null;// data graph, including vertice IDs, edge IDs, and their link relationships
	private int vertexType[] = null;// vertex -> type
	private int edgeType[] = null;// edge -> type
	private MetaPath queryMPath = null;// the query meta-path
	private double[] weight = null;
	public HomoGraphBuilder(int graph[][], int vertexType[], int edgeType[], MetaPath queryMPath, double[] weight) {
		this.graph = graph;
		this.vertexType = vertexType;
		this.edgeType = edgeType;
		this.queryMPath = queryMPath;
		this.weight = weight;
	}
	
	public BatchSearch build(Set<Integer> keepSet) {
		BatchSearch affVertexFinder = new BatchSearch(graph, vertexType, edgeType, queryMPath, weight);
		if (queryMPath.edge.length == 2) {
			for (int startId : keepSet) {affVertexFinder.collect2Type(startId, keepSet);}
		} else if (queryMPath.edge.length == 4) {
			int cnt = 0;
			for (int startId : keepSet) {
				cnt += 1
				affVertexFinder.collect3Type(startId, keepSet, cnt == keepSet.size());
			}
		}
		return affVertexFinder;
	}
	

	
	
	
	public static void main(String[] args) {
		int vertex1[] = { 1, 0, 3, 0, 1 }, edge1[] = { 3, 2, 5, 0 };
		MetaPath metaPath1 = new MetaPath(vertex1, edge1);
		
		DataReader dataReader = new DataReader(Config.dblpGraph, Config.dblpVertex, Config.dblpEdge, Config.dblpWeight);
		int graph[][] = dataReader.readGraph();
		int vertexType[] = dataReader.readVertexType();
		int edgeType[] = dataReader.readEdgeType();
//		double weight[] = dataReader.readWeight();
		
//		FastBCore BKCore = new FastBCore(graph, vertexType, edgeType);
//		Set<Set<Integer>> conSet = BKCore.query(metaPath1, 50);
//		System.out.println("con" + conSet);
//		Set<Integer> keepSet = new HashSet<Integer>();
//		for (Set<Integer> subSet : conSet) {keepSet.addAll(subSet);}
//		System.out.println(conSet.size());
//		System.out.println(keepSet.size());
////		Map<double[], Set<Integer>> result = new HashMap<double[], Set<Integer>>();
//		BatchSearch affVertexFinder = new BatchSearch(graph, vertexType, edgeType, metaPath1);
//
//		for (Set<Integer> type1 : conSet) {
//			System.out.println("type1size" + type1.size());
//			affVertexFinder.collect3Type(16987, type1);
//			}
		}
}
