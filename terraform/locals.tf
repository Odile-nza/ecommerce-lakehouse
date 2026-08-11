locals {
  products_job_name    = "process-products"
  orders_job_name      = "process-orders"
  order_items_job_name = "process-order-items"
  crawler_name         = "${var.project_name}-dwh-crawler"
  sns_topic_name       = "${var.project_name}-alerts"

  # Table names doubling as the Athena-queryable name once the crawler
  # registers each Delta location as a native Delta table.
  products_table    = "products"
  orders_table      = "orders"
  order_items_table = "order_items"

  prefixes = [
    "raw/products/",
    "raw/orders/",
    "raw/order_items/",
    "rejected/products/",
    "rejected/orders/",
    "rejected/order_items/",
    "lakehouse-dwh/products/",
    "lakehouse-dwh/orders/",
    "lakehouse-dwh/order_items/",
    "archived/products/",
    "archived/orders/",
    "archived/order_items/",
    "scripts/",
    "athena-results/",
  ]
}
