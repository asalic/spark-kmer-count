package upv.i3m.spark_kmer_count;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.graphx.*;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.PropertyConfigurator;

import scala.Tuple2;



public class App 
{
	//static public final int kmerSize = 15;
	//static public long mask = 0;
	
	static Logger logger = Logger.getLogger(App.class.getName());
	
	static int MIN_KLST_SZ = 2;
	
	static public class Params implements Serializable
	{

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		protected int kmerSize = 15;
		protected long mask = 0;
		protected int gapOpen;
		protected int gapExt;
		protected int match;
		protected int mismatch;
		protected int estmCov;
		protected int minKLstSz;
		
		
		public int getKmerSize() {
			return kmerSize;
		}
		public void setKmerSize(int kmerSize) {
			this.kmerSize = kmerSize;
		}
		public long getMask() {
			return mask;
		}
		public void setMask(long mask) {
			this.mask = mask;
		}
		public int getGapOpen() {
			return gapOpen;
		}
		public void setGapOpen(int gapOpen) {
			this.gapOpen = gapOpen;
		}
		public int getGapExt() {
			return gapExt;
		}
		public void setGapExt(int gapExt) {
			this.gapExt = gapExt;
		}
		public int getMatch() {
			return match;
		}
		public void setMatch(int match) {
			this.match = match;
		}
		public int getMismatch() {
			return mismatch;
		}
		public void setMismatch(int mismatch) {
			this.mismatch = mismatch;
		}
		public int getEstmCov() {
			return estmCov;
		}
		public void setEstmCov(int estmCov) {
			this.estmCov = estmCov;
		}
		public int getMinKLstSz() {
			return minKLstSz;
		}
		public void setMinKLstSz(int minKLstSz) {
			this.minKLstSz = minKLstSz;
		}
		
		
		
	}
	
	static class IdsLstIdAggregate 
		implements Function2<ArrayList<Integer>, Integer, ArrayList<Integer>>
	{
		public ArrayList<Integer> call(ArrayList<Integer> lstIds, Integer id)
				throws Exception 
		{
			lstIds.add(id);
			return lstIds;
		}
		
	}
	
	static class IdsLstIdsLstAggregate 
	implements Function2<ArrayList<Integer>, ArrayList<Integer>, ArrayList<Integer>>
	{
		public ArrayList<Integer> call(ArrayList<Integer> lstIds1, ArrayList<Integer> lstIds2)
				throws Exception 
		{
			lstIds1.addAll(lstIds2);
			return lstIds1;
		}
	
	}
	
	
	
    public static void main( String[] args )
    {
    	ConsoleAppender ca = new ConsoleAppender();
    	ca.setWriter(new OutputStreamWriter(System.out));
    	ca.setLayout(new PatternLayout("%-5p [%t]: %m%n"));
    	logger.addAppender(ca);
    	logger.setLevel(Level.INFO);
    	logger.info("KABOOM");
    	if (args.length != 4)
    	{
    		System.err.println("Not enough arguments");
    		System.exit(1);
    	}
    	long stopTime, elapsedTime, startTimeTmp;
    	int kSize = Integer.parseInt(args[2]);
    	final int estmCov = Integer.parseInt(args[3]);
    	Params p = new Params();
    	p.setKmerSize(kSize);
    	long mask = 0;
    	for (int idx=0; idx<kSize-1; ++idx)
    		mask = (mask << 2) + 3;
    	p.setMask(mask);
    	System.out.println("*************Mask is: " + Long.toBinaryString(mask));
    	String inPath = args[0];
    	String outPath = args[1];
    	try {
    		File outF = new File(outPath);
			FileUtils.deleteDirectory(outF);
			//outF.mkdirs();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	//outPath += "/" + (System.currentTimeMillis() / 1000);
    	
    	SparkConf conf = new SparkConf().setAppName("ASM").setMaster("local");//spark://localhost:7077");                                        
        conf.set("spark.local.dir", "~/tmp/"); 
        conf.set("spark.executor.memory", "4G");
        JavaSparkContext sc = new JavaSparkContext(conf);
    	sc.setLocalProperty("spark.executor.memory", "4G");
    	System.setProperty("spark.executor.cores", "4");
    	System.setProperty("spark.default.parallelism", "local[2]");


    	p.setEstmCov(estmCov);
    	p.setMinKLstSz(2);
    	final Broadcast<Params> br = sc.broadcast(p);
    	long startTime = System.currentTimeMillis();
    	JavaRDD<String> lines = sc.textFile(inPath);
    	lines.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK()); // maybe it's better to have a cache should enough memory is available on the nodes
    	
    	//System.out.println("*************Extract reads <id, str>**************");
    	//startTimeTmp = System.currentTimeMillis();
        JavaPairRDD<Integer, String> idsReadsMap = lines.mapToPair(new IdsReadsMap());
        idsReadsMap.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK_2()); // replicate on two nodes
        //stopTime = System.currentTimeMillis();
         //elapsedTime = stopTime - startTimeTmp;
        //System.out.println("*************Extract reads <id, str> time = " + (elapsedTime / 1000) + "**************");

    	//System.out.println("*************Extract k-mers**************");
    	//startTimeTmp = System.currentTimeMillis();
    	//KmerExtract.p = p;
        JavaPairRDD<Long, Integer> kmerRead = lines.flatMapToPair(new KmerExtract(br));
        lines.unpersist();
        //stopTime = System.currentTimeMillis();
        //elapsedTime = stopTime - startTimeTmp;
       // System.out.println("*************Extract k-mers time = " + (elapsedTime / 1000) + "**************");
        
    	        
    	//System.out.println("*************Aggregate k-mers**************");
    	//startTimeTmp = System.currentTimeMillis();
        JavaPairRDD<Long, ArrayList<Integer>> kmerIdsLst = 
        		kmerRead.aggregateByKey(new ArrayList<Integer>(), 
        				new IdsLstIdAggregate(), new IdsLstIdsLstAggregate());
        //stopTime = System.currentTimeMillis();
        //elapsedTime = stopTime - startTimeTmp;
        //System.out.println("*************Aggregate k-mers time = " + (elapsedTime / 1000) + "**************");
        
    	//System.out.println("*************Filter k-mers**************");
    	//startTimeTmp = System.currentTimeMillis();
        JavaPairRDD<Long, ArrayList<Integer> > filteredRdd = 
        		kmerIdsLst.filter(new Function<scala.Tuple2<Long, ArrayList<Integer> >, Boolean> ()
        				{
        					public Boolean call(scala.Tuple2<Long, ArrayList<Integer> > row)
        					{
        						//System.out.println(br.getValue().getEstmCov());
        						if (row._2.size() > br.getValue().getMinKLstSz() && row._2.size() < br.getValue().getEstmCov() * 3)
        							return true;
        						return false;
        					}
        				}
        	);
        //stopTime = System.currentTimeMillis();
        //elapsedTime = stopTime - startTimeTmp;
        //System.out.println("*************Filter k-mers time = " + (elapsedTime / 1000) + "**************");
        
        //System.out.println("*************Fast check linking reads**************");
    	//startTimeTmp = System.currentTimeMillis();
        JavaPairRDD<Integer, Integer> kmerIdsLstChecked = 
        		kmerIdsLst.flatMapToPair(new FilterFastReadsLinks());
        //stopTime = System.currentTimeMillis();
        //elapsedTime = stopTime - startTimeTmp;
        //System.out.println("*************Fast check linking reads time = " + (elapsedTime / 1000) + "**************");

               
        //System.out.println("*************Save k-mers**************");
    	//startTimeTmp = System.currentTimeMillis();
    	filteredRdd.saveAsTextFile(outPath); 
    	//stopTime = System.currentTimeMillis();
         //elapsedTime = stopTime - startTimeTmp;
        //System.out.println("*************Save k-mers time = " + (elapsedTime / 1000) + "**************");
        

/*        SparkContext scScala = new SparkContext(conf);
        
        RDD<Tuple2<Integer, String> > reads = JavaRDD.toRDD(
        		sc.parallelize(Arrays.asList(new Tuple2<Integer, String>(1, ""),
        		new Tuple2<Integer, String>(2, ""),
        		new Tuple2<Integer, String>(3, ""),
        		new Tuple2<Integer, String>(4, ""),
        		new Tuple2<Integer, String>(5, ""),
        		new Tuple2<Integer, String>(6, ""),
        		new Tuple2<Integer, String>(7, ""),
        		new Tuple2<Integer, String>(8, "")))
        		);
        
        RDD<Edge<String> > relationships = JavaRDD.toRDD(
        		sc.parallelize(Arrays.asList(new Edge<String>(1, 2, ""),
        		new Edge<String>(2, 3, ""),
        		new Edge<String>(3, 4, ""),
        		new Edge<String>(4, 5, ""),
        		new Edge<String>(5, 6, "")
        		))
        		);
        Graph g = Graph.apply(reads, relationships, "", StorageLevel.MEMORY_AND_DISK_2(), StorageLevel.MEMORY_AND_DISK_2(), 
        		scala.reflect.ClassTag$.MODULE$.apply(String.class),
        		scala.reflect.ClassTag$.MODULE$.apply(String.class));
    	*/
    	
      stopTime = System.currentTimeMillis();
      elapsedTime = stopTime - startTime;
        System.out.println("*************Happy ending after (total time): " + (elapsedTime / 1000));
    	//List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);                                                             
    	//JavaRDD<Integer> distData = sc.parallelize(data); 
    	//int sum = distData.reduce(new Sum());
    	sc.close();
        //System.out.println("The sum is: " + sum);
    }
}

//static class ReadsAligner 
//implements Function2<String, String, >
//{
//protected static int NW(String s1, String s2) {
//
//	// It is easier to index and faster to use
//	// integer matrix for fixed length storage
//	int[][] scoreMatrix = new int[s1.length() + 1][s2.length() + 1];
//
//	int gapPenalty = br.getValue().getGapOpen();
//	int substitutePenalty = br.getValue().getMismatch();
//	int similarityCost = br.getValue().getMatch();
//
//	AlignmentResult result = new AlignmentResult();
//	result.setParameters(parameters);
//
//	// Initialize the score matrix
//	// the first row and column are for the gap
//	// Complexity: O(NxM)
//	for (int i = 0; i < s1.length() + 1; i++) {
//		for (int j = 0; j < s2.length() + 1; j++) {
//			scoreMatrix[i][j] = 0;
//			if (i == 0)
//				scoreMatrix[i][j] = gapPenalty * j;
//			else if (j == 0)
//				scoreMatrix[i][j] = gapPenalty * i;
//		}
//	}
//	int matchCost = 0;
//	int seq1GapCost = 0;
//	int seq2GapCost = 0;
//
//	// Compute the minimum cost scores between all
//	// possible pairs of prefixes
//	// Complexity: O(NxM)
//	for (int i = 1; i < s1.length() + 1; i++) {
//		for (int j = 1; j < s2.length() + 1; j++) {
//
//			// Case 1: The cost of mistmatch between the two prefixes
//			similarityCost = (s2.charAt(j - 1) == s1.charAt(i - 1)) ? 0 : substitutePenalty;
//
//			matchCost = scoreMatrix[i - 1][j - 1] + similarityCost;
//
//			// Case 2: the cost of adding a gap on sequence 2
//			seq2GapCost = scoreMatrix[i - 1][j] + gapPenalty;
//
//			// Case 3: the cost of adding a gap on sequence 1
//			seq1GapCost = scoreMatrix[i][j - 1] + gapPenalty;
//
//			scoreMatrix[i][j] = Math.min(Math.min(matchCost, seq1GapCost), seq2GapCost);
//		}
//	}
//	// Reconstruct the Alignment by backtracking on
//	// the score matrix
//	// Complexity O(N+M)
//	StringBuilder alignedSequence1 = new StringBuilder();
//	StringBuilder alignedSequence2 = new StringBuilder();
//
//	int j = s2.length();
//	int i = s1.length();
//
//	while (i > 0 || j > 0) {
//		if (i > 0 && j > 0)
//			similarityCost = (s2.charAt(j - 1) == s1.charAt(i - 1)) ? 0 : substitutePenalty;
//		else
//			similarityCost = Integer.MAX_VALUE;
//
//		if (i > 0 && j > 0 && scoreMatrix[i][j] == scoreMatrix[i - 1][j - 1] + similarityCost) {
//			alignedSequence1.append(s1.charAt(i - 1));
//			alignedSequence2.append(s2.charAt(j - 1));
//			i = i - 1;
//			j = j - 1;
//		} else if (i > 0 && scoreMatrix[i][j] == scoreMatrix[i - 1][j] + gapPenalty) {
//			alignedSequence2.append("-");
//			alignedSequence1.append(s1.charAt(i - 1));
//			i = i - 1;
//		} else if (j > 0 && scoreMatrix[i][j] == scoreMatrix[i][j - 1] + gapPenalty) {
//			alignedSequence1.append("-");
//			alignedSequence2.append(s2.charAt(j - 1));
//			j = j - 1;
//		}
//	} // end of while
//
//	result.setTotalCost(scoreMatrix[s1.length()][s2.length()]);
//	result.setAlignmentLength(alignedSequence1.length());
//	result.setAlignments(
//			new String[] { alignedSequence1.reverse().toString(), alignedSequence2.reverse().toString() });
//
//	return result;
//}
//
//public String call(String arg0) throws Exception {
//	// TODO Auto-generated method stub
//	return null;
//}
//}

//static public class Sum implements Function2<Integer, Integer, Integer>
//{
//
//	/**
//	 * 
//	 */
//	private static final long serialVersionUID = 1L;
//
//	public Integer call(Integer a, Integer b) throws Exception {
//		return a + b;
//	}
//	
//}
