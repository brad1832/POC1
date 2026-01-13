scala
import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object InvSalesAggJob {

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

    val spark = SparkSession.builder
      .appName(s"Inventory Sales Aggregation - $targetDate")
      .getOrCreate()

    try {
      // 1. Read Sales Data (equivalent to SalesMapper)
      // Assuming Sales Parquet has columns: Item_Identifier, Outlet_Identifier, Quantity
      val salesDF = spark.read.parquet(salesInputPath)
        .select(
          F.col("Item_Identifier").as("item_identifier"),
          F.col("Outlet_Identifier").as("outlet_identifier"),
          F.col("Quantity").as("quantity_sold")
        )

      // 2. Read Inventory Data (equivalent to InventoryMapper)
      // Assuming Inventory Parquet has columns: Item_Identifier, Outlet_Identifier, Stock_Quantity
      val inventoryDF = spark.read.parquet(inventoryInputPath)
        .select(
          F.col("Item_Identifier").as("item_identifier"),
          F.col("Outlet_Identifier").as("outlet_identifier"),
          F.col("Stock_Quantity").as("stock_quantity")
        )

      // 3. Join and Aggregate (equivalent to InvSalesAggReducer)
      // Perform an inner join on item_identifier and outlet_identifier
      // Then group by item_identifier, outlet_identifier, and stock_quantity
      // (assuming stock_quantity is unique per item/outlet for a given date)
      // and sum the sold_quantity.
      val resultDF = salesDF
        .join(inventoryDF, Seq("item_identifier", "outlet_identifier"), "inner")
        .groupBy("item_identifier", "outlet_identifier", "stock_quantity")
        .agg(F.sum("quantity_sold").as("sold_quantity"))
        .select("item_identifier", "outlet_identifier", "stock_quantity", "sold_quantity") // Ensure final schema matches

      // 4. Write Output to Parquet
      resultDF.write
        .mode("overwrite") // Overwrite if output path already exists
        .option("compression", "snappy") // Equivalent to ParquetOutputFormat.setCompression
        .parquet(outputPath)

      println(s"\n=== Job Completed Successfully! ===")
      println(s"Output written to: $outputPath")
      System.exit(0)

    } catch {
      case e: Exception =>
        System.err.println(s"\n=== Job Failed! ===")
        e.printStackTrace()
        System.exit(1)
    } finally {
      spark.stop()
    }
  }
}