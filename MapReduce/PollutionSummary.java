package PollutionSummary;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PollutionSummary {

  public static class PollutionMapper
       extends Mapper<Object, Text, Text, DataList>{
	  
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String line = value.toString();
      String[] fields = line.split(",");
      String time = fields[7];
      int ozone = Integer.parseInt(fields[0].trim());
      int particullate_matter = Integer.parseInt(fields[1].trim());
      int carbon_monoxide = Integer.parseInt(fields[2].trim());
      int sulfure_dioxide = Integer.parseInt(fields[3].trim());
      int nitrogen_dioxide = Integer.parseInt(fields[4].trim());
      
      context.write(new Text(time), new DataList(ozone,particullate_matter,carbon_monoxide,sulfure_dioxide,nitrogen_dioxide));
    }
  }

  public static class PollutionReducer
       extends Reducer<Text,DataList,Text,DataList> {

    public void reduce(Text key, Iterable<DataList> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sumO = 0;
      int sumPM = 0;
      int sumCM = 0;
      int sumSD = 0;
      int sumND = 0;
      for (DataList data : values) {
        sumO += data.getOzone();
        sumPM += data.getPM();
        sumCM += data.getCM();
        sumSD += data.getSD();
        sumND += data.getND();
      }
      DataList result = new DataList(sumO,sumPM,sumCM,sumSD,sumND);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration  conf = new Configuration();
    Job job = Job.getInstance(conf);
    job.setJarByClass(PollutionSummary.class);
    job.setMapperClass(PollutionMapper.class);
    job.setCombinerClass(PollutionReducer.class);
    job.setReducerClass(PollutionReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DataList.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}