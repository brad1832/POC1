scala
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object InvSalesAggJob {

  // The output schema defined in the original MapReduce job.
  // Spark DataFrames infer schema, but this serves as a reference
  // for the expected structure of the Parquet files (excluding the partition column).
  private val OUTPUT_SCHEMA_STRING =
    "message inv_sales_agg {" +
      " required binary item_identifier (UTF8);" +
      " required binary outlet_identifier (UTF8);" +
      " required int64 stock_quantity;" +
      " required int64 sold_quantity;" +
      "}"

  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      System.err.println("Usage: InvSalesAggJob <sales_base_path> <inventory_base_path> <output_base_path> <date>")
      System.err.println("Example: InvSalesAggJob /data/sales /data/inventory /output/inv_sales_agg 2023-07-02")
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

    // Construct full paths with date, mirroring the MapReduce driver logic
    val salesInputPath = s"$salesBasePath/sale_date=$targetDate"
    val inventoryInputPath = s"$inventoryBasePath/date=$targetDate"
    val outputPath = s"$outputBasePath/event_date=$targetDate" // This is the full partition path

    println("=== Job Configuration ===")
    println(s"Sales Input Path: $salesInputPath")
    println(s"Inventory Input Path: $inventoryInputPath")
    println(s"Output Path: $outputPath")
    println(s"Target Date: $targetDate")
    println("========================")

    val spark = SparkSession.builder
      .appName(s"Inventory Sales Aggregation - $targetDate")
      .getOrCreate()

    import spark.implicits._

    try {
      // --- Sales Mapper Semantics ---
      // Read sales data from the specified Parquet path.
      val salesDF = spark.read.parquet(salesInputPath)

      // Transform sales data:
      // - Select and rename columns to match the desired output structure.
      // - Group by item_identifier and outlet_identifier (the composite key from SalesMapper).
      // - Aggregate (sum) the 'Quantity' to get 'sold_quantity', mimicking the reducer's aggregation for sales.
      val salesAggDF = salesDF
        .select(
          col("Item_Identifier").as("item_identifier"),
          col("Outlet_Identifier").as("outlet_identifier"),
          col("Quantity").as("quantity")
        )
        .groupBy("item_identifier", "outlet_identifier")
        .agg(sum("quantity").as("sold_quantity"))
        .as("sales") // Alias for clarity in the join operation

      // --- Inventory Mapper Semantics ---
      // Read inventory data from the specified Parquet path.
      val inventoryDF = spark.read.parquet(inventoryInputPath)

      // Transform inventory data:
      // - Select and rename columns.
      // - Group by item_identifier and outlet_identifier (the composite key from InventoryMapper).
      // - Aggregate 'Stock_Quantity' using `max`. The MR reducer would take the last value encountered
      //   if multiple inventory records existed for the same key. `max` provides a deterministic
      //   aggregation in Spark for this scenario.
      val inventoryCleanDF = inventoryDF
        .select(
          col("Item_Identifier").as("item_identifier"),
          col("Outlet_Identifier").as("outlet_identifier"),
          col("Stock_Quantity").as("stock_quantity")
        )
        .groupBy("item_identifier", "outlet_identifier")
        .agg(max("stock_quantity").as("stock_quantity")) // Max for deterministic aggregation
        .as("inventory") // Alias for clarity in the join operation

      // --- Reducer Semantics ---
      // Perform an inner join between the aggregated sales and inventory DataFrames.
      // This directly implements the reducer's "if (hasInventory && hasSales)" condition,
      // ensuring that only records with both sales and inventory data for a given
      // item_identifier and outlet_identifier are included in the final output.
      val joinedDF = salesAggDF.join(
        inventoryCleanDF,
        Seq("item_identifier", "outlet_identifier"),
        "inner"
      )

      // Add the 'event_date' column, which is used for partitioning the output.
      // This mirrors the MapReduce job's output path structure.
      val finalDF = joinedDF
        .withColumn("event_date", lit(targetDate))
        .select(
          col("item_identifier"),
          col("outlet_identifier"),
          col("stock_quantity"),
          col("sold_quantity"),
          col("event_date") // Include for partitioning
        )

      // --- Output Writing ---
      // Write the resulting DataFrame to Parquet format.
      // - `mode("overwrite")`: Overwrites the output directory if it exists.
      // - `option("compression", "snappy")`: Sets the Parquet compression codec to Snappy.
      // - `partitionBy("event_date")`: Organizes output into date-based partitions.
      // - `parquet(outputBasePath)`: Specifies the base output directory.
      finalDF.write
        .mode("overwrite")
        .option("compression", "snappy")
        .partitionBy("event_date")
        .parquet(outputBasePath)

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