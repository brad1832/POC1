package org.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.parquet.example.data.Group;

import java.io.IOException;

public class SalesMapper extends Mapper<LongWritable, Group, Text, Text> {



    private String targetDate;
    private Text outputKey = new Text();
    private Text outputValue = new Text();


    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        targetDate = context.getConfiguration().get("target.date")
    }



    @Override
    public void map(LongWritable key, Group value, Context context)
             throws IOException, InterruptedException {


	  String itemIdentifier = value.getString(field:"Item_Identifier", index:0);
	  String outletIdentifier = value.getString(field:"Outlet_Identifier", index:0);
	  long quantity = value.getLong(field:"Quantity", index:0);

	  //Composite key: item_identifier|outlet_identifier
	  outputKey.set(itemIdentifier + "|" + outletIdentifier);

          // Value format: S|sale_date|quantity (S for Sales)
          outputValue.set("S|" + targetDate + "|" + quantity);

          context.write(outputKey, outputValue);
	}
}