import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/* stats class is used pass and hold min/max values */
public class Stats implements Writable{
	
	private double TEMP_MISSING = 9999.9;
	private double WIND_MISSING = 999.9;
	private double MaxTemp; 
	private double MinTemp; 
	private double MaxWind; 
	private double MinWind;
	private String Stations;
	
	public Stats(){
	}
	
	public Stats(String stn, double mnt, double mxt, double mnw, double mxw){
		this.Stations = stn;
		this.MinTemp = (mnt != TEMP_MISSING && mnt != WIND_MISSING) ? mnt : Double.MAX_VALUE;
		this.MaxTemp = (mxt != TEMP_MISSING && mxt != WIND_MISSING) ? mxt : Double.MIN_VALUE;
		this.MinWind = mnw != WIND_MISSING ? mnw : Double.MAX_VALUE;
		this.MaxWind = mxw != WIND_MISSING ? mxw : Double.MIN_VALUE;
	}
	
	public Double getMinTemp(){
		return this.MinTemp;
	}
	
	public void setMinTemp(double min){
		this.MinTemp = min;
	}
	
	public Double getMaxTemp(){
		return this.MaxTemp;
	}
	
	public void setMaxTemp(double max){
		this.MaxTemp = max;
	}
	
	public Double getMinWind(){
		return this.MinWind;
	}
	
	public void setMinWind(double min){
		this.MinWind = min;
	}
	
	public Double getMaxWind(){
		return this.MaxWind;
	}
	
	public void setMaxWind(double max){
		this.MaxWind = max;
	}
	
	public String getStation(){
		return this.Stations;
	}
	
	public void setStation(String stn){
		this.Stations = stn;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.Stations);
		out.writeDouble(this.MinTemp);
		out.writeDouble(this.MaxTemp);
		out.writeDouble(this.MinWind);
		out.writeDouble(this.MaxWind);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.Stations = in.readUTF();
		this.MinTemp = in.readDouble();
		this.MaxTemp = in.readDouble();
		this.MinWind = in.readDouble();
		this.MaxWind = in.readDouble();
	}
	
	@Override
	public String toString(){
		String[] stns = Stations.split(",");
		return "\nStation: " + stns[0] + " Min Temp: " + MinTemp + "\t" + 
				   "\nStation: " + stns[1] + " Max Temp: " + MaxTemp + "\t" +
				   "\nStation: " + stns[2] + " Min Wind: " + MinWind + "\t" +
				   "\nStation: " + stns[3] + " Max Wind: " + MaxWind;
	}
}

