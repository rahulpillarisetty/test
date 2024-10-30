
-- Common date calculations for reuse
WITH current_month AS (
    SELECT DATE_TRUNC('month', CURRENT_DATE) as first_day,
           DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' - INTERVAL '1 day' as last_day
),
previous_month AS (
    SELECT DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') as first_day,
           DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 day' as last_day
)

-- 1. Top 5 brands by receipts scanned for most recent month
SELECT 
    b.name as brand_name,
    COUNT(DISTINCT r.receipt_id) as receipt_count
FROM receipts r
JOIN receipt_items ri ON r.receipt_id = ri.receipt_id
JOIN brands b ON ri.brand_id = b.brand_id
WHERE r.date_scanned BETWEEN (SELECT first_day FROM current_month)
    AND (SELECT last_day FROM current_month)
GROUP BY b.brand_id, b.name
ORDER BY receipt_count DESC
LIMIT 5;

-- 2. Compare rankings between current and previous month
WITH current_month_ranks AS (
    SELECT 
        b.name as brand_name,
        COUNT(DISTINCT r.receipt_id) as receipt_count,
        RANK() OVER (ORDER BY COUNT(DISTINCT r.receipt_id) DESC) as current_rank
    FROM receipts r
    JOIN receipt_items ri ON r.receipt_id = ri.receipt_id
    JOIN brands b ON ri.brand_id = b.brand_id
    WHERE r.date_scanned BETWEEN (SELECT first_day FROM current_month)
        AND (SELECT last_day FROM current_month)
    GROUP BY b.brand_id, b.name
),
previous_month_ranks AS (
    SELECT 
        b.name as brand_name,
        COUNT(DISTINCT r.receipt_id) as receipt_count,
        RANK() OVER (ORDER BY COUNT(DISTINCT r.receipt_id) DESC) as previous_rank
    FROM receipts r
    JOIN receipt_items ri ON r.receipt_id = ri.receipt_id
    JOIN brands b ON ri.brand_id = b.brand_id
    WHERE r.date_scanned BETWEEN (SELECT first_day FROM previous_month)
        AND (SELECT last_day FROM previous_month)
    GROUP BY b.brand_id, b.name
)
SELECT 
    COALESCE(c.brand_name, p.brand_name) as brand_name,
    c.receipt_count as current_month_receipts,
    c.current_rank,
    p.receipt_count as previous_month_receipts,
    p.previous_rank,
    c.current_rank - p.previous_rank as rank_change
FROM current_month_ranks c
FULL OUTER JOIN previous_month_ranks p ON c.brand_name = p.brand_name
WHERE c.current_rank <= 5 OR p.previous_rank <= 5
ORDER BY COALESCE(c.current_rank, p.previous_rank);

-- 3. Average spend comparison by receipt status
SELECT 
    receipt_status,
    COUNT(*) as receipt_count,
    AVG(total_spent) as avg_spend
FROM receipts
WHERE receipt_status IN ('Accepted', 'Rejected')
GROUP BY receipt_status
ORDER BY avg_spend DESC;

-- 4. Brand spend analysis for recent users (past 6 months)
WITH recent_users AS (
    SELECT user_id
    FROM users
    WHERE created_date >= CURRENT_DATE - INTERVAL '6 months'
)
SELECT 
    b.name as brand_name,
    COUNT(DISTINCT r.receipt_id) as transaction_count,
    SUM(ri.final_price * ri.quantity) as total_spend
FROM recent_users ru
JOIN receipts r ON ru.user_id = r.user_id
JOIN receipt_items ri ON r.receipt_id = ri.receipt_id
JOIN brands b ON ri.brand_id = b.brand_id
GROUP BY b.brand_id, b.name
ORDER BY total_spend DESC
LIMIT 5;

-- Recommended indexes for performance
CREATE INDEX idx_receipts_date_scanned ON receipts(date_scanned);
CREATE INDEX idx_receipts_status ON receipts(receipt_status);
CREATE INDEX idx_users_created_date ON users(created_date);
CREATE INDEX idx_receipt_items_receipt_brand ON receipt_items(receipt_id, brand_id);
