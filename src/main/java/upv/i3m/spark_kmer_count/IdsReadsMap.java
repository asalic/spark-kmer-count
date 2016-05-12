package upv.i3m.spark_kmer_count;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class IdsReadsMap 
	implements PairFunction<String, Integer, String>
{

	public Tuple2<Integer, String> call(String entry) 
			throws Exception 
	{
		String fields[] = entry.split("\t");
		Integer rId = Integer.parseInt(fields[0]);
		return new Tuple2<Integer, String>(rId, fields[1]);
	}

}
