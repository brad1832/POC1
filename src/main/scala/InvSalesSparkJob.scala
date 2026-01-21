scala
package org.example

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object InvSalesAggSpark {

  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      System.err.println("Usage: InvSalesAggSpark <sales_base_path> <inventory_base_path> <output_base_path> <date>")
      System.err.println("Example: InvSalesAggSpark /data/sales /data/inventory /output/inv_sales_agg 2023-07-02")
      System.err.println("Will read from:")
      System.err.println(" -/data/sales/sale_date=<date>")
      System.err.println(" -/data/inventory/date=<date>")
      System.err.println(" -Output to: /output/inv_sales_agg/event_date=<date>")
      System.exit(1)
    }

    val salesBasePath = args(0)
    val inventoryBasePath = args(1)
    val outputBasePath = args(2)
    val targetDate = args(3)

    // Construct full paths with date
    val salesInputPath = s"$salesBasePath/sale_date=$targetDate"
    val inventoryInputPath = s"$inventoryBasePath/date=$targetDate"
    val outputPath = s"$outputBasePath" // Output path will be partitioned by event_date

    println("=== Job Configuration ===")
    println(s"Sales Input Path: $salesInputPath")
    println(s"Inventory Input Path: $inventoryInputPath")
    println(s"Output Base Path: $outputPath")
    println(s"Target Date: $targetDate")
    println("========================")

    val spark = SparkSession.builder
      .appName(s"Inventory Sales Aggregation - $targetDate")
      .getOrCreate()

    import spark.implicits._

    try {
      // 1. Process Sales Data (mimics SalesMapper)
      val salesDF: DataFrame = spark.read.parquet(salesInputPath)
        .select(
          col("Item_Identifier").as("item_identifier"),
          col("Outlet_Identifier").as("outlet_identifier"),
          col("Quantity").cast("long").as("quantity_sales")
        )
        .withColumn("source_type", lit("S")) // Mark as Sales record
        .withColumn("stock_quantity_raw", lit(0L)) // Placeholder for union
        .withColumn("join_key", concat_ws("|", $"item_identifier", $"outlet_identifier"))
        .select("join_key", "source_type", "quantity_sales", "stock_quantity_raw")

      // 2. Process Inventory Data (mimics InventoryMapper)
      val inventoryDF: DataFrame = spark.read.parquet(inventoryInputPath)
        .select(
          col("Item_Identifier").as("item_identifier"),
          col("Outlet_Identifier").as("outlet_identifier"),
          col("Stock_Quantity").cast("long").as("quantity_inventory")
        )
        .withColumn("source_type", lit("I")) // Mark as Inventory record
        .withColumn("quantity_sales", lit(0L)) // Placeholder for union
        .withColumn("join_key", concat_ws("|", $"item_identifier", $"outlet_identifier"))
        .select("join_key", "source_type", "quantity_sales", "quantity_inventory")

      // 3. Union both DataFrames (mimics Map output collection)
      val unionedDF = salesDF.unionByName(inventoryDF)

      // 4. Group and Aggregate (mimics InvSalesAggReducer)
      val aggregatedDF = unionedDF
        .groupBy("join_key")
        .agg(
          sum(when(col("source_type") === "S", col("quantity_sales")).otherwise(0L)).as("sold_quantity"),
          // For stock_quantity, the MR reducer takes the value from the 'I' record.
          // Assuming there's only one 'I' record per join_key, max will correctly pick it up.
          max(when(col("source_type") === "I", col("quantity_inventory")).otherwise(null)).as("stock_quantity"),
          countDistinct(col("source_type")).as("distinct_sources")
        )
        // Apply the "inner join" semantic: only output if both inventory ('I') and sales ('S') records were found
        .filter(col("distinct_sources") === 2)

      // 5. Final selection and column renaming
      val finalDF = aggregatedDF
        .withColumn("item_identifier", split(col("join_key"), "\\|").getItem(0))
        .withColumn("outlet_identifier", split(col("join_key"), "\\|").getItem(1))
        .withColumn("event_date", lit(targetDate)) // Add event_date for partitioning

      // Select the required output columns and write to Parquet
      finalDF.select("item_identifier", "outlet_identifier", "stock_quantity", "sold_quantity", "event_date")
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .partitionBy("event_date") // Partition by event_date as per MR job
        .parquet(outputPath)

      println("\n=== Job Completed Successfully! ===")
      println(s"Output written to: $outputPath/event_date=$targetDate")

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