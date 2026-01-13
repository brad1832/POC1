package org.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.parquet.example.data.Group;

import java.io.IOException;

public class InventoryMapper extends Mapper<LongWritable, Group, Text, Text> {



    private Text outputKey = new Text();
    private Text outputValue = new Text();



    @Override
    public void map(LongWritable key, Group value, Context context)
             throws IOException, InterruptedException {


	  String itemIdentifier = value.getString(field:"Item_Identifier", index:0);
	  String outletIdentifier = value.getString(field:"Outlet_Identifier", index:0);
	  long stockQuantity = value.getLong(field:"Stock_Quantity", index:0);

	  //Composite key: item_identifier|outlet_identifier
	  outputKey.set(itemIdentifier + "|" + outletIdentifier);

          // Value format: S|sale_date|quantity (S for Sales)
          outputValue.set("I|" + stockQuantity);

          context.write(outputKey, outputValue);
	}
}