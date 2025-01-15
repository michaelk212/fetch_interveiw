SELECT
    i.BrandCode2 AS Brand,
    SUM(i.finalPrice) AS TotalSpend
FROM
    items i
JOIN
    receipts r ON i.receipt_id = r.receipt_id
JOIN
    users u ON r.user_id = u.user_id
WHERE
    u.createdDate >= (SELECT DATE(MAX(createdDate), '-6 months') FROM users)
    AND i.BrandCode2 IS NOT NULL
    AND TRIM(i.BrandCode2) != ''
    AND i.finalPrice IS NOT NULL
GROUP BY
    i.BrandCode2
ORDER BY
    TotalSpend DESC
LIMIT 1;
