import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class NaturalJoin extends Configured implements Tool {

    @Override public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.printf("Usage: %s [genericOptions] %s\n\n",
                              getClass().getSimpleName(),
                              "<R input> <S input> <output");
            GenericOptionsParser.printGenericCommandUsage(System.err);
            return -1;
        }

        Job job = new Job(getConf(), "Join records from R and S");
        job.setJarByClass(getClass());

        Path rInputPath = new Path(args[0]);
        Path sInputPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);

        MultipleInputs.addInputPath(job,
                                    rInputPath,
                                    KeyValueTextInputFormat.class,
                                    NaturalJoinRMapper.class);
        MultipleInputs.addInputPath(job,
                                    sInputPath,
                                    KeyValueTextInputFormat.class,
                                    NaturalJoinSMapper.class);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TextDuple.class);
        job.setReducerClass(NaturalJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new NaturalJoin(), args);
        System.exit(result);
    }
}
