-- Which 5 products generate the highest revenue?
SELECT TOP 5
     p.product_name,
     SUM(s.sales_amount) AS total_revenue
FROM gold.fact_sales s
LEFT JOIN gold.dim_products p
ON s.product_key= p.product_key
GROUP BY product_name
ORDER BY total_revenue DESC



/*    Using Window Function   */

SELECT *
FROM 
(
    SELECT 
         p.product_name,
         SUM(s.sales_amount) AS total_revenue,
         ROW_NUMBER() OVER(ORDER BY SUM(s.sales_amount)DESC ) AS rank_products
    FROM gold.fact_sales s
    LEFT JOIN gold.dim_products p
    ON s.product_key= p.product_key
    GROUP BY product_name
)t
WHERE rank_products <= 5


-- What are the 5 worst-performing products in terms of sales?
SELECT TOP 5
    p.product_name,
    SUM(s.sales_amount) AS total_revenue
FROM gold.fact_sales s
LEFT JOIN gold.dim_products p
ON p.product_key = s.product_key
GROUP BY product_name
ORDER BY total_revenue

-- Find the top 10 customers who have generated the highest revenue
SELECT TOP 10
    c.customer_key,
    c.first_name,
    c.last_name,
    SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON f.customer_key=c.customer_key
GROUP BY  c.customer_key,
    c.first_name,
    c.last_name
ORDER BY total_revenue DESC


-- The 3 customers with the fewest orders placed

SELECT TOP 3
    c.customer_key,
    c.first_name,
    c.last_name,
    COUNT(DISTINCT f.order_number) AS total_orders
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON f.customer_key=c.customer_key
GROUP BY  c.customer_key,
    c.first_name,
    c.last_name
ORDER BY total_orders 

