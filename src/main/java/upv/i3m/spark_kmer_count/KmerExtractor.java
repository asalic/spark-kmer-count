package upv.i3m.spark_kmer_count;

import java.util.HashSet;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;
import upv.i3m.spark_kmer_count.App.Params;

class KmerExtract implements PairFlatMapFunction<String, Long, Integer>
{
	protected Params params;
	
	public KmerExtract(Broadcast<Params> params)
	{
		this.params = params.getValue();
		//System.out.println("++++++++++++++++++++++constructor called");
	}

	//static Params p;
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public Iterable<Tuple2<Long, Integer>> call(String read) throws Exception 
	{
		HashSet<Tuple2<Long, Integer>> result = new HashSet<Tuple2<Long, Integer>>();
		//ArrayList<Tuple2<Long, Integer>> result = new ArrayList<Tuple2<Long, Integer>>();
		String[] recs = read.split("\t");
		Integer rId = Integer.parseInt(recs[0]);
    	//System.out.println(params.getKmerSize());
		long kmer = 0;
		int kSizeNow = 0;
    	//Params p = new Params();
    	//KmerExtract.p.setKmerSize(kSize);    	
		int kmerSize = params.getKmerSize();
		long mask = params.getMask();
    	for (int idx=0; idx<kmerSize-1; ++idx)
    		mask = (mask << 2) + 3;
		for (int idx=0; idx<recs[1].length(); ++idx) 
		{
			if (kSizeNow == kmerSize)
			{
				result.add(new Tuple2<Long, Integer>(kmer, rId));
				kmer &= mask;
			} else
			{
				++kSizeNow;
			}
			kmer = (kmer << 2) + baseAToB(recs[1].charAt(idx));
		}
		return result;
	}
	
	public static int baseAToB(char base)
	{
		switch (base)
		{
			case 'A':
			case 'a': return 0;
			case 'C':
			case 'c': return 1;
			case 'G':
			case 'g': return 2;
			case 'T':
			case 't': return 3;
			case 'N':
			case 'n': return 4;
			default: return -1;			
		}
	}
	
}
