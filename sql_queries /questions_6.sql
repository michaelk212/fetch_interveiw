SELECT
    i.BrandCode2 AS Brand,
    COUNT(i.BrandCode2) AS TransactionCount
FROM
    items i
JOIN
    receipts r ON i.receipt_id = r.receipt_id
JOIN
    users u ON r.userId = u.user_id
WHERE
    u.createdDate >= (SELECT DATE(MAX(createdDate), '-6 months') FROM users)
    AND i.BrandCode2 IS NOT NULL
    AND TRIM(i.BrandCode2) != ''
GROUP BY
    i.BrandCode2
ORDER BY
    TransactionCount DESC
LIMIT 1;
