package ca.mcit.cricri.doublejoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * Input: <Common field(s), a record> => <Text, Text>
 * a record is represented in Text (string) as concatenating all fields with '\\t' character.
 * For example: [a, 1] => "a\\t1"
 *
 * Input example:
 * From R
 * a	1 =====> <a, "R	a	1">
 * a	2 =====> <a, "R	a	2">
 * From S
 * a	i =====> <a, "S	a	i">
 */
public class DoubleJoinReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        // Split input in two list each of which representing one relation (left or right)
        List<String> leftRelation = new ArrayList<>();
        List<String> rightRelation = new ArrayList<>();
        for(Text value : values) {
            String record = value.toString();
            String[] fields = record.split("\t");
            String relationName = fields[0];
            if (relationName.equals("R")) leftRelation.add(record);
            else rightRelation.add(record);
        }
        if (rightRelation.isEmpty() || leftRelation.isEmpty()) return;
        // "Put it together" using cartesian product
        for(String recordA : leftRelation) {
            for (String recordB: rightRelation) {
                String recordOut = recordA + "\t" + recordB;
                // Commit
                context.write(key, new Text(recordOut));
            }
        }
    }
}