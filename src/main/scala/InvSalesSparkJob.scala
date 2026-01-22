scala
package org.example

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object InvSalesAggSparkJob {

  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      System.err.println("Usage: InvSalesAggSparkJob <sales_base_path> <inventory_base_path> <output_base_path> <date>")
      System.err.println("Example: InvSalesAggSparkJob /data/sales /data/inventory /output/inv_sales_agg 2023-07-02")
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

    // Construct full paths with date
    val salesInputPath = s"$salesBasePath/sale_date=$targetDate"
    val inventoryInputPath = s"$inventoryBasePath/date=$targetDate"
    val outputPath = s"$outputBasePath/event_date=$targetDate"

    println("=== Job Configuration ===")
    println(s"Sales Input Path: $salesInputPath")
    println(s"Inventory Input Path: $inventoryInputPath")
    println(s"Output Path: $outputPath")
    println(s"Target Date: $targetDate")
    println("========================")

    val spark = SparkSession.builder()
      .appName(s"Inventory Sales Aggregation - $targetDate")
      .getOrCreate()

    import spark.implicits._

    try {
      // 1. Read Inventory Data (Mapper semantics for InventoryMapper)
      // Select relevant columns and rename to match desired output schema
      val inventoryDF: DataFrame = spark.read.parquet(inventoryInputPath)
        .select(
          col("Item_Identifier").as("item_identifier"),
          col("Outlet_Identifier").as("outlet_identifier"),
          col("Stock_Quantity").as("stock_quantity")
        )

      // 2. Read Sales Data (Mapper semantics for SalesMapper)
      // Select relevant columns and rename for aggregation
      val salesDF: DataFrame = spark.read.parquet(salesInputPath)
        .select(
          col("Item_Identifier").as("item_identifier"),
          col("Outlet_Identifier").as("outlet_identifier"),
          col("Quantity").as("sold_quantity")
        )

      // 3. Aggregate Sales Quantities (Part of Reducer semantics)
      // Group by item and outlet, then sum the sold quantities
      val aggregatedSalesDF: DataFrame = salesDF
        .groupBy("item_identifier", "outlet_identifier")
        .agg(sum("sold_quantity").as("sold_quantity"))

      // 4. Join Inventory and Aggregated Sales (Reducer semantics: inner join and final projection)
      // The inner join ensures that only records with both inventory and sales are included,
      // mirroring the `if (hasInventory && hasSales)` condition in the Reducer.
      val resultDF: DataFrame = inventoryDF
        .join(aggregatedSalesDF, Seq("item_identifier", "outlet_identifier"), "inner")
        .select(
          col("item_identifier"),
          col("outlet_identifier"),
          col("stock_quantity"),
          col("sold_quantity")
        )

      // 5. Write Output
      // The output schema is implicitly handled by the DataFrame's schema.
      // ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY) is handled by option("compression", "snappy")
      resultDF
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(outputPath)

      println("\n=== Job Completed Successfully! ===")
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