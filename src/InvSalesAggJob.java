package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;


public class InvSalesAggJob extends Configured implements Tool {


      // Output schema without event_date (only 4 columns)
      private static final String OUPUT_SCHEMA = 
	      "message inv_sales_agg {" +
                      " required binary item_identifier (UTF8);" +
	              " required binary outlet_identifier (UTF8);" +
                      " required int64 stock_quantity;" +
                      " required int64 sold_quantity;" +
                      "}";

      @Override
      public int run(String[] args) throws Exception {
          if (args.length != 4) {
             System.err.println("Usage: InvSalesAggJob <sales_base_path> <inventory_base_path> <output_base_path> <date>");
             System.err.println("Example: InvSalesAggJob /date/sales /data/inventory /output/inv_sales_agg 2023-07-02");
             System.err.println("Will read from:"); 
	     System.err.println(" -/data/sales/sale_date=<date>");
             System.err.println(" -/data/inventory/date=<date>");
	     System.err.println(" -Output to: /output/inv_sales_agg/event_date=<date>");
             return 1;
         }

         String salesBasePath = args[0];
         String inventoryBasePath = args[1];
         String outputBasePath = args[2];
         String targetDate = args[3];

         // Construct full paths with date
         String salesInputPath = salesBasePath + "/sale_date=" + targetDate;
         String inventoryInputPath = inventoryBasePath + "/date=" + targetDate;
         String outputPath = outputBasePath + "/event_date=" + targetDate; 

         System.out.println("=== Job Configuration ==="); 
         System.out.println("Sales Input Path: " + salesInputPath); 
         System.out.println("Inventory Input Path: " + inventoryInputPath);
         System.out.println("Output Path: " + outputPath);
         System.out.println("Target Date: " + targetDate);
         System.out.println("========================");

         Configuration conf = getConf():
         conf.set("target.date", targetDate);
	 conf.set("parquet.output.schema", OUTPUT_SCHEMA);
	 conf.set("parquet.read.support.class", "org.apache.parquet.hadoop.example.GroupReadSupport");

     	 Job job = Job.getInstance(conf, jobName:"Inventory Sales Aggregation - " + targetDate);
 	 job.setJarByClass(InvSalesAggJob.class);

	 //Configure Input
	 job.getConfiguration().set("parquet.read.support.class", "org.apache.parquet.hadoop.example.GroupReadSupport");

	 //Multiple inputs with date-based paths
	 MultipleInputs.addInputPath(job, new Path(salesInputPath),
		ParquetInputFormat.class, SalesMapper.class);
	 MultipleInputs.addInputPath(job, new Path(inventoryInputPath),
		ParquetInputFormat.class, InventoryMapper.class);

	//Configure reducer
	job.setReducerClass(InvSalesAggReducer.class);
	job.setNumReduceTasks(10);

	//Map output types
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);

	//Set output format to Parquet
	job.setOutputFormatClass(ParquetOutputFormat.class);
	job.setOutputKeyClass(Void.class);
	job.setOutputValueClass(org.apache.parquet.example.data.simple.SimpleGroup.class);

	//Configure Parquet Output
	ParquetOutputFormat.setOutputPath(job, new Path(outputPath));
	ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
	ParquetOutputFormat.setWriteSupportClass(job, org.apache.parquet.hadoop.example.GroupWriteSupport.class);

	MessageType schema = MessageTypeParser.parseMessageType(OUTPUT_SCHEMA);
	org.apache.parquet.hadoop.example.GroupWrite.Support.setSchema(schema, job.getConfiguration());

	boolean success = job.waitForCompletion(verbose: true);

	if (success) {
	   System.out.println("\n=== Job Completed Successfully! ===");
	   System.out.println("Output written to: " + outputPath);
	} else {
	    System.err.println("\n=== Job Failed! ===");
	}

	return success ? 0 : 1;
     }
     
     public static void main(String[] args) throws Exception {
	int exitCode = ToolRunner.run(new Configuration(), new InvSalesAggJob(), args);
	System.exit(exitCode);
     }
}



         

