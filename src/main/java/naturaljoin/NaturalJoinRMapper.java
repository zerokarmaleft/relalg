import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NaturalJoinRMapper
    extends Mapper<Text, Text, Text, TextDuple> {

    @Override protected void map(Text key,
                                 Text value,
                                 Context context)
        throws IOException, InterruptedException {

        System.err.printf("R: (%s => %s)\n", key, value);
        // key-value pair of (value, ("R", key)) is used to denote
        // (b, (R, a)) where R is a relation
        context.write(value,
                      new TextDuple(new Text("R"), key));
    }
}
