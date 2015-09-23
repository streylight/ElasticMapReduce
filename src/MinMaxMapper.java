import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/* this class is responsible for mapping */
public class MinMaxMapper extends
		Mapper<Object, Text, Text, Stats> {

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		/* parse needed values from the input line */
		String[] split = value.toString().split("\\s+");
		
		/* skip title line and any line with invalid station number */
		if (split[3].compareTo("TEMP,") != 0 && split[0].compareTo("999999") != 0){
			String year = split[2].substring(0, 4);
			
			double temp = Double.parseDouble(split[3]);
			double minWind = Double.parseDouble(split[12]);
			double maxWind = Double.parseDouble(split[14]);
	
			Stats outStat = new Stats(split[0],temp, temp, minWind, maxWind);
			
			/* write output with year as the key for the reducer */
			context.write(new Text(year), outStat);
		}

	}
}