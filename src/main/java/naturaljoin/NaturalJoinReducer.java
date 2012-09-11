import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NaturalJoinReducer
    extends Reducer<Text, TextDuple, Text, Text> {

    @Override protected void reduce(Text key,
                                    Iterable<TextDuple> values,
                                    Context context)
        throws IOException, InterruptedException {

        ArrayList<String> rTuples = new ArrayList<String>();
        ArrayList<String> sTuples = new ArrayList<String>();
        for (TextDuple value : values) {
            if (value.getFirst().toString().equals("R")) {
                rTuples.add(value.getSecond().toString());
            }
            if (value.getFirst().toString().equals("S")) {
                sTuples.add(value.getSecond().toString());
            }
            // else malformed value
        }

        for (String rTuple : rTuples) {
            for (String sTuple : sTuples) {
                context.write(key,
                              new Text(rTuple +
                                       "\t" +
                                       sTuple));
            }
        }
    }
}
