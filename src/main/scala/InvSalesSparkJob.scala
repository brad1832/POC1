scala
package org.example

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Spark Scala application to aggregate inventory and sales data.
 * This application mirrors the logic of the provided Hadoop MapReduce job.
 *
 * It reads sales and inventory data for a specific date, joins them
 * on item and outlet identifiers, aggregates quantities, and writes
 * the result to a Parquet file.
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

    // Parse arguments
    val salesBasePath = args(0)
    val inventoryBasePath = args(1)
    val outputBasePath = args(2)
    val targetDate = args(3)

    // Construct full paths with date partitions
    val salesInputPath = s"$salesBasePath/sale_date=$targetDate"
    val inventoryInputPath = s"$inventoryBasePath/date=$targetDate"
    val outputPath = s"$outputBasePath/event_date=$targetDate"

    // Print job configuration
    println("=== Job Configuration ===")
    println(s"Sales Input Path: $salesInputPath")
    println(s"Inventory Input Path: $inventoryInputPath")
    println(s"Output Path: $outputPath")
    println(s"Target Date: $targetDate")
    println("========================")

    // Initialize SparkSession
    val spark = SparkSession.builder()
      .appName(s"Inventory Sales Aggregation - $targetDate")
      .getOrCreate()

    // Import Spark implicits for DataFrame operations (e.g., .as, .toDF)
    import spark.implicits._

    try {
      // 1. Process Sales Data
      // This step is equivalent to the SalesMapper and the sales-related aggregation in the Reducer.
      // It reads sales records, extracts identifiers and quantity, and sums quantities for
      // each unique (item_identifier, outlet_identifier) pair.
      val salesDF: DataFrame = spark.read.parquet(salesInputPath)
        .select(
          col("Item_Identifier").as("item_identifier"),
          col("Outlet_Identifier").as("outlet_identifier"),
          col("Quantity").as("sold_quantity")
        )
        .groupBy("item_identifier", "outlet_identifier")
        .agg(sum("sold_quantity").as("sold_quantity")) // Sum quantities for multiple sales records for the same key

      // 2. Process Inventory Data
      // This step is equivalent to the InventoryMapper and the inventory-related logic in the Reducer.
      // It reads inventory records, extracts identifiers and stock quantity.
      // The Reducer's logic of `stockQuantity = Long.parseLong(parts[1])` implies taking one value
      // if multiple exist for a key (the last one processed). `first` is used here for deterministic selection.
      val inventoryDF: DataFrame = spark.read.parquet(inventoryInputPath)
        .select(
          col("Item_Identifier").as("item_identifier"),
          col("Outlet_Identifier").as("outlet_identifier"),
          col("Stock_Quantity").as("stock_quantity")
        )
        .groupBy("item_identifier", "outlet_identifier")
        .agg(first("stock_quantity").as("stock_quantity")) // Take one stock quantity per item/outlet

      // 3. Join Sales and Inventory Data
      // The Reducer's condition `if (hasInventory && hasSales)` translates directly to an inner join.
      // This ensures that only records with both sales and inventory information are included.
      val aggregatedDF: DataFrame = salesDF.join(
        inventoryDF,
        Seq("item_identifier", "outlet_identifier"), // Join on common identifiers
        "inner" // Perform an inner join
      )
      // Select and order final columns as per the desired output schema
      .select(
        col("item_identifier"),
        col("outlet_identifier"),
        col("stock_quantity"),
        col("sold_quantity")
      )

      // 4. Write Output to Parquet
      // The MR job specifies ParquetOutputFormat with Snappy compression.
      // Spark's DataFrame writer handles this directly.
      aggregatedDF.write
        .mode("overwrite") // Overwrite the output directory if it already exists
        .option("compression", "snappy") // Set Snappy compression codec
        .parquet(outputPath)

      println("\n=== Job Completed Successfully! ===")
      println(s"Output written to: $outputPath")

    } catch {
      case e: Exception =>
        System.err.println("\n=== Job Failed! ===")
        e.printStackTrace()
        System.exit(1) // Exit with a non-zero code on failure
    } finally {
      spark.stop() // Stop the SparkSession
    }
  }
}