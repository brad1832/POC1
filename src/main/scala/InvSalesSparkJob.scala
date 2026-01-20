scala
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object InvSalesAggJob {

  def main(args: Array[String]): Unit = {

    // 1. Argument Parsing and Validation (Driver equivalent)
    if (args.length != 4) {
      System.err.println("Usage: InvSalesAggJob <sales_base_path> <inventory_base_path> <output_base_path> <date>")
      System.err.println("Example: InvSalesAggJob /data/sales /data/inventory /output/inv_sales_agg 2023-07-02")
      System.err.println("Will read from:")
      System.err.println(" - /data/sales/sale_date=<date>")
      System.err.println(" - /data/inventory/date=<date>")
      System.err.println(" - Output to: /output/inv_sales_agg/event_date=<date>")
      System.exit(1)
    }

    val salesBasePath = args(0)
    val inventoryBasePath = args(1)
    val outputBasePath = args(2)
    val targetDate = args(3)

    val salesInputPath = s"$salesBasePath/sale_date=$targetDate"
    val inventoryInputPath = s"$inventoryBasePath/date=$targetDate"
    val outputPath = s"$outputBasePath/event_date=$targetDate"

    println("=== Job Configuration ===")
    println(s"Sales Input Path: $salesInputPath")
    println(s"Inventory Input Path: $inventoryInputPath")
    println(s"Output Path: $outputPath")
    println(s"Target Date: $targetDate")
    println("========================")

    // 2. Initialize SparkSession
    val spark = SparkSession.builder
      .appName(s"Inventory Sales Aggregation - $targetDate")
      .getOrCreate()

    import spark.implicits._

    try {
      // 3. Sales Data Processing (SalesMapper equivalent)
      // Reads sales data, extracts relevant fields, and marks source as 'S'.
      // Corresponds to SalesMapper's map method output: (item_identifier|outlet_identifier, S|targetDate|quantity)
      val salesDF = spark.read.parquet(salesInputPath)
        .select(
          col("Item_Identifier").as("item_identifier"),
          col("Outlet_Identifier").as("outlet_identifier"),
          col("Quantity").as("sold_quantity_input") // Renamed to avoid conflict with aggregated column
        )
        .withColumn("source", lit("S"))
        .withColumn("stock_quantity_input", lit(null).cast(LongType)) // Placeholder for union compatibility
        .select("item_identifier", "outlet_identifier", "source", "stock_quantity_input", "sold_quantity_input")

      // 4. Inventory Data Processing (InventoryMapper equivalent)
      // Reads inventory data, extracts relevant fields, and marks source as 'I'.
      // Corresponds to InventoryMapper's map method output: (item_identifier|outlet_identifier, I|stock_quantity)
      val inventoryDF = spark.read.parquet(inventoryInputPath)
        .select(
          col("Item_Identifier").as("item_identifier"),
          col("Outlet_Identifier").as("outlet_identifier"),
          col("Stock_Quantity").as("stock_quantity_input") // Renamed to avoid conflict with aggregated column
        )
        .withColumn("source", lit("I"))
        .withColumn("sold_quantity_input", lit(null).cast(LongType)) // Placeholder for union compatibility
        .select("item_identifier", "outlet_identifier", "source", "stock_quantity_input", "sold_quantity_input")

      // 5. Union Mapped Data (Combines outputs from both mappers)
      // This creates a single DataFrame where each row represents a mapped record.
      val unionedDF = salesDF.unionByName(inventoryDF)

      // 6. Reducer Logic (InvSalesAggReducer equivalent)
      // Groups by the composite key (item_identifier, outlet_identifier) and aggregates values.
      val aggregatedDF = unionedDF
        .groupBy("item_identifier", "outlet_identifier")
        .agg(
          // For stock_quantity, we expect only one 'I' record per key, so max() or first() works.
          max(col("stock_quantity_input")).as("stock_quantity"),
          // For sold_quantity, we sum all 'S' records for the key.
          sum(col("sold_quantity_input")).as("sold_quantity"),
          // Flags to check if both inventory ('I') and sales ('S') records were present for the group.
          max(when(col("source") === "I", 1).otherwise(0)).as("has_inventory"),
          max(when(col("source") === "S", 1).otherwise(0)).as("has_sales")
        )
        // 7. Filter: Only output if both inventory and sales records exist for the key (inner join semantics)
        .filter(col("has_inventory") === 1 && col("has_sales") === 1)
        // 8. Select final output columns matching the desired schema
        .select(
          col("item_identifier"),
          col("outlet_identifier"),
          col("stock_quantity"),
          col("sold_quantity")
        )

      // 9. Write Output (ParquetOutputFormat equivalent)
      aggregatedDF.write
        .mode("overwrite")
        .option("compression", "snappy") // Corresponds to ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY)
        .parquet(outputPath)

      println(s"\n=== Job Completed Successfully! ===")
      println(s"Output written to: $outputPath")

    } catch {
      case e: Exception =>
        System.err.println("\n=== Job Failed! ===")
        e.printStackTrace()
        System.exit(1)
    } finally {
      spark.stop()
    }
  }
}