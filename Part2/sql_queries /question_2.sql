WITH previous_month AS (
    SELECT strftime('%Y-%m', DATE(MAX(r.dateScanned), '-1 month')) AS month
    FROM receipts r
    JOIN items i ON i.receipt_id = r.receipt_id
    WHERE i.BrandCode2 IS NOT NULL AND TRIM(i.BrandCode2) != '' AND r.dateScanned IS NOT NULL
)
SELECT
    i.BrandCode2 AS Brand,
    COUNT(i.receipt_id) AS ReceiptCount
FROM
    items i
JOIN
    receipts r ON i.receipt_id = r.receipt_id
WHERE
    i.BrandCode2 IS NOT NULL
    AND TRIM(i.BrandCode2) != ''
    AND r.dateScanned IS NOT NULL
    AND strftime('%Y-%m', r.dateScanned) = (SELECT month FROM previous_month)
GROUP BY
    i.BrandCode2
ORDER BY
    ReceiptCount DESC
LIMIT 5;
