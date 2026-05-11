# Scaling reflection

If headline volume grew a thousand-fold, the part that breaks first is
the trio of `outputMode("complete")` aggregations writing to memory
sinks. Complete mode forces Spark to re-emit the entire result set on
every micro-batch, and the memory sink materialises that result inside
the driver's heap so Streamlit can read it with `spark.sql`. At
today's volume the tables are tiny; at 1000x they push the driver
into GC pressure and then OOM, long before the cluster workers do.

The right Spark feature to reach for is append-mode windowed
aggregations under a tight watermark, written through `foreachBatch`
to a partitioned external store (Delta Lake or Kafka topic). State is
pruned by the watermark, the per-trigger full re-emit disappears, and
the dashboard reads from the external store instead of pinning every
row in driver memory.
