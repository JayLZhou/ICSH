package util;
import java.io.File;

import javax.swing.filechooser.FileSystemView;

/**
 * @author fangyixiang
 * @date 2018-09-10 Global parameters
 */
public class Config {
	// stem file paths
	public static String stemFile = "./stemmer.lowercase.txt";
	public static String stopFile = "./stopword.txt";

	// the root of date files
//	public static String root = "/data/hancwang/YIxing/HINData";
//	public static String root = "C:\\Users\\fangyixiang\\Desktop";
//	public static String root = "D:\\UNSW\\HINData\\";
	public static String root = "/mnt/data/zyl/data"; // for yingli
//	public static String root = "/mnt/A/zyl/data/"; // for yangqin

	{
		FileSystemView fsv = FileSystemView.getFileSystemView();
		File com = fsv.getHomeDirectory();
		root = com.getPath();// automatically obtain the path of Desktop
	}

	// SmallDBLP
	public static String smallDBLPRoot = root + "\\HIN\\dataset\\SmallDBLP\\";
	public static String smallDBLPGraph = smallDBLPRoot + "graph.txt";
	public static String smallDBLPVertex = smallDBLPRoot + "vertex.txt";
	public static String smallDBLPEdge = smallDBLPRoot + "edge.txt";

	// DBLP
//	public static String dblpRoot = root + "/dataset/TopkSmallDBLPWithWeight/";
	public static String dblpRoot = root + "/DBLPWithWeight/";
//	public static String dblpRoot = root + "/dataset/dblp/";
	public static String dblpGraph = dblpRoot + "graph.txt";
	public static String dblpVertex = dblpRoot + "vertex.txt";
	public static String dblpEdge = dblpRoot + "edge.txt";
	public static String dblpWeight = dblpRoot + "weight.txt";

	//PubMed

	public static String pubmedRoot = root + "/dataset/PubMed/";
	public static String pubmedGraph = pubmedRoot + "graph.txt";
	public static String pubmedVertex = pubmedRoot + "vertex.txt";
	public static String pubmedEdge = pubmedRoot + "edge.txt";
	public static String pubmedWeight = pubmedRoot + "weight.txt";
	// IMDB
//	public static String IMDBRoot = root + "\\HIN\\dataset\\yearIMDB\\";
//	public static String IMDBGraph = IMDBRoot + "graph.txt";
//	public static String IMDBVertex = IMDBRoot + "vertex.txt";
//	public static String IMDBEdge = IMDBRoot + "edge.txt";
	public static String IMDBRoot = root + "/dataset/IMDB/";
	public static String IMDBGraph = IMDBRoot + "graph.txt";
	public static String IMDBVertex = IMDBRoot + "vertex.txt";
	public static String IMDBEdge = IMDBRoot + "edge.txt";
	public static String IMDBWeight = IMDBRoot + "weight.txt";
	// Foursquare
	public static String FsqRoot = root + "/dataset/FourSquare/";
	public static String FsqGraph = FsqRoot + "graph.txt";
	public static String FsqVertex = FsqRoot + "vertex.txt";
	public static String FsqEdge = FsqRoot + "edge.txt";

	// DBpedia
	public static String dbpediaRoot = root + "/DBPedia/";
	public static String dbpediaGraph = dbpediaRoot + "graph.txt";
	public static String dbpediaVertex = dbpediaRoot + "vertex.txt";
	public static String dbpediaEdge = dbpediaRoot + "edge.txt";

	public static String machineName = "Phoenix19";
//	public static String logFinalResultFile = Config.root + "/outdata/" + machineName;//our final experimental result data
	public static String logFinalResult2File = "./log2";// our final experimental result data
	public static String logFinalResult3File = "./log3";// our final experimental result data
	public static String logPartResultFile =  "./outdata/" + machineName + "-part";// intermediate result
	public static String logAna2ResultFile = "./outdata/" + "analysize2" + "-part";;
	public static String logAna3ResultFile = "./outdata/" + "analysize3" + "-part";;

	// FreeBase
//	public static String FreeBaseRoot = "/Users/zhouxiaolun/Downloads/HINTrussICDE2020/FreeBase/";
	public static String FreeBaseRoot = root + "/dataset/FreeBase.txt/";
	public static String FreeBaseGraph = FreeBaseRoot + "graph.txt";
	public static String FreeBaseVertex = FreeBaseRoot + "vertex.txt";
	public static String FreeBaseEdge = FreeBaseRoot + "edge.txt";
	public static String FreeBaseWeight = FreeBaseRoot + "weight.txt";
	public static int k = 6;

	// music
	public static String musicRoot = root + "/dataset/music/";
	public static String musicGraph = musicRoot + "graph.txt";
	public static String musicVertex = musicRoot + "vertex.txt";
	public static String musicEdge = musicRoot + "edge.txt";
	public static String musicWeight = musicRoot + "weight.txt";

	// tmdb
	// music
	public static String tmdbRoot = root + "/tmdb/";
	public static String tmdbGraph = tmdbRoot + "graph.txt";
	public static String tmdbVertex = tmdbRoot + "vertex.txt";
	public static String tmdbEdge = tmdbRoot + "edge.txt";
	public static String tmdbWeight = tmdbRoot + "weight.txt";
}
