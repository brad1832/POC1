package org.example;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.parquet.example.data.simple,SimpleGroup;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;


public class InvSalesAggReducer extends Reducer<Text, Text, Void, SimpleGroup> {

    private MessageType schema;
  
    @Override
    protected void setup(Context context) throws IOException, InterruptedExcption {
         String schemaString = context.getConfiguration().get("parquet.output.schema");
	 schema = org.apache.parquet.schema.MessageTypeParser.parseMessageType(schemaString);
    }


    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {


         String[] keyParts = key.toString().split(regex:"\\|");
	 String itemIdentifier = keyParts[0];
	 String outltIdentifier = keyParts[1];

	 long stockQuantity = 0L;
	 long soldQuantity = 0L;
	 boolean hasInventory = false;
	 boolean hasSales = false;

	 //Process all values for this key
	 for (Text value: values) {
	     String[] parts = value.toString().split(regex:"\\|");
	     String source = parts[0];

	     if ("I".equals(source)) {
	         //Iventory record
		 stockQuantity = Long.parseLong(parts[1]);
		 hasInventory = true;
	     } else if ("S".equals(source)) {
		 // Sales record - sum the quantities
		 long quantity = Long.parseLong(parts[2]);
		 soldQuantity += quantity;
		 hasSales = true;
	    }
        }


        // Only output if we have both inventory and sales (inner join)
	if (hasInventory && hasSales) {
	    SimpleGroup group = new SimpleGroup(schema);
	    group.add(field:"item_identifier", itemIdentifier);
	    group.add(field:"outlet_identifier", outletIdentifier);
	    group.add(field:"stock_quantity", stockQuantity);
	    group.add(field:"sold_quantity", soldQuantity);
	    // No event_date added

	    context.write(keyout:null, group);
        }
    }
}

