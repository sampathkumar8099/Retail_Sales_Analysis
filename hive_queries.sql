-- ============================================================
-- Retail Sales Analytics - Hive Queries
-- ============================================================

-- ------------------------------------------------------------
-- 1. Verify External Table
-- ------------------------------------------------------------
SHOW TABLES LIKE 'retail_fact_orders';

DESCRIBE FORMATTED retail_fact_orders;

-- ------------------------------------------------------------
-- 2. Basic Row Count Validation
-- ------------------------------------------------------------
SELECT COUNT(*) AS total_records
FROM retail_fact_orders;

-- ------------------------------------------------------------
-- 3. Revenue Analysis
-- ------------------------------------------------------------

-- Total Revenue
SELECT SUM(price) AS total_revenue
FROM retail_fact_orders;

-- Revenue by Product Category
SELECT product_category_name,
       SUM(price) AS revenue
FROM retail_fact_orders
GROUP BY product_category_name
ORDER BY revenue DESC;

-- ------------------------------------------------------------
-- 4. Top Products & Sellers
-- ------------------------------------------------------------

-- Top 10 Products by Revenue
SELECT product_id,
       SUM(price) AS revenue
FROM retail_fact_orders
GROUP BY product_id
ORDER BY revenue DESC
LIMIT 10;

-- Top 10 Sellers by Revenue
SELECT seller_id,
       SUM(price) AS revenue
FROM retail_fact_orders
GROUP BY seller_id
ORDER BY revenue DESC
LIMIT 10;

-- ------------------------------------------------------------
-- 5. Payment Analysis
-- ------------------------------------------------------------

-- Revenue by Payment Type
SELECT payment_type,
       COUNT(DISTINCT order_id) AS total_orders,
       SUM(payment_value) AS total_payment
FROM retail_fact_orders
GROUP BY payment_type
ORDER BY total_payment DESC;

-- Average Payment Value by Payment Type
SELECT payment_type,
       AVG(payment_value) AS avg_payment_value
FROM retail_fact_orders
GROUP BY payment_type;

-- ------------------------------------------------------------
-- 6. Customer & Order Insights
-- ------------------------------------------------------------

-- Orders per Customer
SELECT customer_id,
       COUNT(DISTINCT order_id) AS total_orders
FROM retail_fact_orders
GROUP BY customer_id
ORDER BY total_orders DESC
LIMIT 10;

-- Average Order Value
SELECT AVG(price) AS avg_order_value
FROM retail_fact_orders;

-- ------------------------------------------------------------
-- 7. Product Category Quality Check
-- ------------------------------------------------------------

-- Orders with Unknown Product Category
SELECT COUNT(*) AS unknown_category_orders
FROM retail_fact_orders
WHERE product_category_name = 'unknown';

-- Revenue from Unknown Categories
SELECT SUM(price) AS unknown_category_revenue
FROM retail_fact_orders
WHERE product_category_name = 'unknown';

-- ------------------------------------------------------------
-- 8. Seller Performance Quality Check
-- ------------------------------------------------------------

-- Sellers with High Revenue
SELECT seller_id,
       SUM(price) AS revenue,
       COUNT(DISTINCT order_id) AS orders
FROM retail_fact_orders
GROUP BY seller_id
HAVING SUM(price) > 100000
ORDER BY revenue DESC;

-- ------------------------------------------------------------
-- 9. Data Validation Queries
-- ------------------------------------------------------------

-- Orders with Multiple Payments
SELECT order_id,
       COUNT(*) AS payment_records
FROM retail_fact_orders
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY payment_records DESC;

-- Check for Negative or Zero Prices
SELECT *
FROM retail_fact_orders
WHERE price <= 0;

-- ------------------------------------------------------------
-- 10. Performance-Friendly Aggregation Example
-- ------------------------------------------------------------

-- Daily Revenue Trend
SELECT DATE(order_purchase_timestamp) AS order_date,
       SUM(price) AS daily_revenue
FROM retail_fact_orders
GROUP BY DATE(order_purchase_timestamp)
ORDER BY order_date;
