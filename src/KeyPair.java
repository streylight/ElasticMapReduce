import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/* key pair class used for tracking top 5 */
public class KeyPair implements Comparable{

	public double value;
	private String station;
	
	public KeyPair(double val, String stn){
		this.value = val;
		this.station = stn;
		
	}
	
	public double getVal(){
		return this.value;
	}
	
	public void setVal(double val){
		this.value = val;
	}
	
	public String getStation(){
		return station;
	}
	
	public void setStation(String stn){
		station = stn;
	}

	@Override
	public int compareTo(Object obj) {
		return Double.compare(this.value, ((KeyPair)obj).getVal());
	}
	
	public String toString(){
		return String.format("%s\t%s", station, value);
	}
}
