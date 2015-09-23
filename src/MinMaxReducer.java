import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map.Entry;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/* this class is responsible for running the reducer */
public class MinMaxReducer extends
		Reducer<Text, Stats, Text, Stats> {
	
	private HashMap<String, Stats> map;
	
	private KeyPair[] maxTemp;
	private KeyPair[] minTemp;
	private KeyPair[] maxWind;
	private KeyPair[] minWind;
	
	/* init the top 5 arrays */
	// ** notes for multi reduce map hash for year. if year existing reassign arrays.  if not create new ones
	public void setup(){
		double maxValue = Double.MIN_VALUE;
		double minValue = Double.MAX_VALUE;
		maxTemp = new KeyPair[5];
		minTemp = new KeyPair[5];
		maxWind = new KeyPair[5];
		minWind = new KeyPair[5];
		
		map = new HashMap<String, Stats>();
		
		for (int i = 0; i < 5; i++){
			maxTemp[i] = new KeyPair(maxValue, " ");
			minTemp[i] = new KeyPair(minValue, " ");
			maxWind[i] = new KeyPair(maxValue, " ");
			minWind[i] = new KeyPair(minValue, " ");
		}
	}
	
	@Override
	public void reduce(Text key, Iterable<Stats> values, Context context)
			throws IOException, InterruptedException {
		
		/* for each key setup a new set of top 5 arrays */
		setup();
		int count = 0;
		for (Stats value : values) {
			/* init new station if one doesnt exist */
			if (map.get(value.getStation()) == null){
				map.put(value.getStation(), value);
			} else {
				Stats stt = new Stats();
				stt.setStation(value.getStation());
				stt.setMaxTemp(Math.max(value.getMaxTemp(), map.get(value.getStation()).getMaxTemp()));
				stt.setMinTemp(Math.min(value.getMinTemp(), map.get(value.getStation()).getMinTemp()));
				stt.setMaxWind(Math.max(value.getMaxWind(), map.get(value.getStation()).getMaxWind()));
				stt.setMinWind(Math.min(value.getMinWind(), map.get(value.getStation()).getMinWind()));
				
				/* update station values */
				map.put(value.getStation(), stt);
			}
		}
		
		/* for each station find the top 5 min/max for the current year */
		for (Entry<String, Stats> entry : map.entrySet()){
			Stats stat = entry.getValue();
			if (stat.getMinTemp() < minTemp[4].getVal()){
				minTemp[4] = new KeyPair(stat.getMinTemp(), stat.getStation());
				Arrays.sort(minTemp);
			}
			
			if (stat.getMaxTemp() > maxTemp[0].getVal()){
				maxTemp[0] = new KeyPair(stat.getMaxTemp(), stat.getStation());
				Arrays.sort(maxTemp);
			}
			
			if (stat.getMinWind() < minWind[4].getVal()){
				minWind[4] = new KeyPair(stat.getMinWind(), stat.getStation());
				Arrays.sort(minWind);
			}
			
			if (stat.getMaxWind() > maxWind[0].getVal()){
				maxWind[0] = new KeyPair(stat.getMaxWind(), stat.getStation());
				Arrays.sort(maxWind);
			}
		}
		
		/* broadcast top 5 for each variable */
		for (int i = 0; i < 5; i++){
			String stns = minTemp[i].getStation() + "," +
					      maxTemp[i].getStation() + "," +
					      minWind[i].getStation() + "," + 
					      maxWind[i].getStation();
			
			context.write(key, new Stats(stns,
										 minTemp[i].getVal(),
										 maxTemp[i].getVal(),
										 minWind[i].getVal(),
										 maxWind[i].getVal()));
		}
	}
}