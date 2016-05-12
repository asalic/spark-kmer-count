package upv.i3m.spark_kmer_count;

import java.util.ArrayList;

import org.apache.spark.SparkEnv;
import org.apache.spark.api.java.function.PairFlatMapFunction;
//import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class FilterFastReadsLinks 
	implements PairFlatMapFunction<Tuple2<Long, ArrayList<Integer> > , 
							Integer, Integer>
{

	/**
	 * 
	 */
	private static final long serialVersionUID = -6637801142740216392L;

	public Iterable<Tuple2<Integer, Integer>>
		call(Tuple2<Long, ArrayList<Integer>> kmerReads) 
				throws Exception 
	{
		
		ArrayList<Tuple2<Integer, Integer> > result = 
				new ArrayList<Tuple2<Integer, Integer> >();
		for (int idxO=0; idxO<kmerReads._2().size() - 1; ++idxO)
			for (int idxI=idxO; idxI<kmerReads._2().size(); ++idxI)
			{
				
			}
		
		return result;
	}

}
