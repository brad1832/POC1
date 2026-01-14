scala
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Spark Scala equivalent of the Hadoop MapReduce InvSalesAggJob.
 *
 * This job reads sales and inventory data (both Parquet files, partitioned by date),
 * joins them based on item_identifier and outlet_identifier, aggregates sales quantities,
 * and writes the result as a new Parquet file.
 *
 * The logic mirrors the original MapReduce job:
 * - Sales data is processed to sum quantities for each item-outlet pair (like SalesMapper + part of Reducer).
 * - Inventory data is processed to get stock quantity for each item-outlet pair (like InventoryMapper).
 * - An inner join combines these two datasets on item_identifier and outlet_identifier (like Reducer's inner join logic).
 * - The final output contains item_identifier, outlet_identifier, stock_quantity, and sold_quantity.
 */
object InvSalesAggJobSpark {

  def main(args: Array[String]): Unit = {
    // Validate command-line arguments
    if (args.length != 4) {
      System.err.println("Usage: InvSalesAggJobSpark <sales_base_path> <inventory_base_path> <output_base_path> <date>")
      System.err.println("Example: InvSalesAggJobSpark /data/sales /data/inventory /output/inv_sales_agg 2023-07-02")
      System.err.println("Will read from:")
      System.err.println(" - /data/sales/sale_date=<date>")
      System.err.println(" - /data/inventory/date=<date>")
      System.err.println(" - Output to: /output/inv_sales_agg/event_date=<date>")
      System.exit(1)
    }

    // Parse command-line arguments
    val salesBasePath = args(0)
    val inventoryBasePath = args(1)
    val outputBasePath = args(2)
    val targetDate = args(3)

    // Construct full input and output paths using the target date
    val salesInputPath = s"$salesBasePath/sale_date=$targetDate"
    val inventoryInputPath = s"$inventoryBasePath/date=$targetDate"
    val outputPath = s"$outputBasePath/event_date=$targetDate"

    // Print job configuration for clarity
    println("=== Job Configuration ===")
    println(s"Sales Input Path: $salesInputPath")
    println(s"Inventory Input Path: $inventoryInputPath")
    println(s"Output Path: $outputPath")
    println(s"Target Date: $targetDate")
    println("========================")

    // Initialize SparkSession
    val spark = SparkSession.builder
      .appName(s"Inventory Sales Aggregation - $targetDate")
      .getOrCreate()

    try {
      // 1. Process Sales Data (equivalent to SalesMapper and sales aggregation in Reducer)
      //    - Reads sales data from the specified path.
      //    - Selects and renames columns to match the desired output format.
      //    - Groups by item_identifier and outlet_identifier to sum up all quantities sold
      //      for that specific item-outlet combination, mirroring the reducer's aggregation logic.
      val salesDF = spark.read.parquet(salesInputPath)
        .select(
          col("Item_Identifier").as("item_identifier"),
          col("Outlet_Identifier").as("outlet_identifier"),
          col("Quantity").as("quantity")
        )
        .groupBy("item_identifier", "outlet_identifier")
        .agg(sum("quantity").as("sold_quantity"))
        // Ensure sold_quantity is LongType as per the original Parquet schema (int64)
        .withColumn("sold_quantity", col("sold_quantity").cast(LongType))

      // 2. Process Inventory Data (equivalent to InventoryMapper)
      //    - Reads inventory data from the specified path.
      //    - Selects and renames columns to match the desired output format.
      val inventoryDF = spark.read.parquet(inventoryInputPath)
        .select(
          col("Item_Identifier").as("item_identifier"),
          col("Outlet_Identifier").as("outlet_identifier"),
          col("Stock_Quantity").as("stock_quantity")
        )
        // Ensure stock_quantity is LongType as per the original Parquet schema (int64)
        .withColumn("stock_quantity", col("stock_quantity").cast(LongType))

      // 3. Join and Final Selection (equivalent to InvSalesAggReducer's join and output logic)
      //    - Performs an inner join between the aggregated sales and inventory DataFrames.
      //      The inner join naturally implements the "if (hasInventory && hasSales)" condition
      //      from the reducer, ensuring only matching item-outlet pairs are included.
      //    - Selects the final columns in the order and names specified by the output schema.
      val aggregatedDF = salesDF.join(
        inventoryDF,
        Seq("item_identifier", "outlet_identifier"), // Join keys
        "inner" // Equivalent to the reducer's inner join logic
      )
      .select(
        col("item_identifier"),
        col("outlet_identifier"),
        col("stock_quantity"),
        col("sold_quantity")
      )

      // 4. Write Output
      //    - Writes the resulting DataFrame to the specified output path in Parquet format.
      //    - Uses "overwrite" mode to handle re-runs.
      //    - Specifies Snappy compression, which is also Spark's default for Parquet.
      aggregatedDF.write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(outputPath)

      println(s"\n=== Job Completed Successfully! ===")
      println(s"Output written to: $outputPath")

    } catch {
      case e: Exception =>
        System.err.println(s"\n=== Job Failed! ===")
        e.printStackTrace()
        System.exit(1) // Exit with a non-zero code to indicate failure
    } finally {
      // Stop the SparkSession to release resources
      spark.stop()
    }
  }
}