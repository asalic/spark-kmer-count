package upv.i3m.spark_kmer_count;

public class Read 
{
	
	protected int id;
	protected String read;
	
	public Read(int id, String r)
	{
		this.id = id;
		this.read = r;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getRead() {
		return read;
	}

	public void setRead(String read) {
		this.read = read;
	}
	
	
}
