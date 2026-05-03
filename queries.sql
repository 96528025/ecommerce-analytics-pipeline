-- ============================================================
-- Olist E-Commerce Analytics
-- ============================================================


-- 1. Monthly GMV (Gross Merchandise Value) trend
SELECT
    d.year,
    d.month,
    d.month_name,
    COUNT(DISTINCT f.order_id)        AS total_orders,
    ROUND(SUM(f.price)::numeric, 2)   AS gmv,
    ROUND(AVG(f.price)::numeric, 2)   AS avg_order_value
FROM fact_order_items f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;


-- 2. Top 10 product categories by revenue
SELECT
    COALESCE(p.category_name, 'Unknown') AS category,
    COUNT(DISTINCT f.order_id)            AS total_orders,
    ROUND(SUM(f.price)::numeric, 2)       AS total_revenue,
    ROUND(AVG(f.review_score)::numeric, 2) AS avg_review_score
FROM fact_order_items f
JOIN dim_products p ON f.product_key = p.product_key
GROUP BY p.category_name
ORDER BY total_revenue DESC
LIMIT 10;


-- 3. Top 10 sellers by revenue
SELECT
    s.seller_id,
    s.seller_city,
    s.seller_state,
    COUNT(DISTINCT f.order_id)       AS total_orders,
    ROUND(SUM(f.price)::numeric, 2)  AS total_revenue
FROM fact_order_items f
JOIN dim_sellers s ON f.seller_key = s.seller_key
GROUP BY s.seller_id, s.seller_city, s.seller_state
ORDER BY total_revenue DESC
LIMIT 10;


-- 4. Revenue and order volume by customer state
SELECT
    c.customer_state,
    COUNT(DISTINCT f.order_id)        AS total_orders,
    COUNT(DISTINCT c.customer_id)     AS unique_customers,
    ROUND(SUM(f.price)::numeric, 2)   AS total_revenue,
    ROUND(AVG(f.price)::numeric, 2)   AS avg_order_value
FROM fact_order_items f
JOIN dim_customers c ON f.customer_key = c.customer_key
GROUP BY c.customer_state
ORDER BY total_revenue DESC;


-- 5. Review score distribution
SELECT
    ROUND(f.review_score) AS score,
    COUNT(*)              AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS percentage
FROM fact_order_items f
WHERE f.review_score IS NOT NULL
GROUP BY ROUND(f.review_score)
ORDER BY score DESC;


-- 6. Month-over-month GMV growth rate
WITH monthly_gmv AS (
    SELECT
        d.year,
        d.month,
        SUM(f.price) AS gmv
    FROM fact_order_items f
    JOIN dim_date d ON f.date_key = d.date_key
    GROUP BY d.year, d.month
)
SELECT
    year,
    month,
    ROUND(gmv::numeric, 2) AS gmv,
    ROUND(
        (gmv - LAG(gmv) OVER (ORDER BY year, month))
        / NULLIF(LAG(gmv) OVER (ORDER BY year, month), 0) * 100
    , 1) AS mom_growth_pct
FROM monthly_gmv
ORDER BY year, month;


-- 7. Freight cost ratio by category (shipping efficiency)
SELECT
    COALESCE(p.category_name, 'Unknown')                        AS category,
    ROUND(AVG(f.freight_value)::numeric, 2)                     AS avg_freight,
    ROUND(AVG(f.price)::numeric, 2)                             AS avg_price,
    ROUND(AVG(f.freight_value / NULLIF(f.price, 0) * 100)::numeric, 1) AS freight_pct
FROM fact_order_items f
JOIN dim_products p ON f.product_key = p.product_key
GROUP BY p.category_name
HAVING COUNT(*) > 100
ORDER BY freight_pct DESC
LIMIT 10;
