SELECT
    CASE
        WHEN r.rewardsReceiptStatus = 'FINISHED' THEN 'Accepted'
        WHEN r.rewardsReceiptStatus = 'REJECTED' THEN 'Rejected'
    END AS Status,
    AVG(r.totalSpent) AS AvgSpend
FROM
    receipts r
WHERE
    r.rewardsReceiptStatus IN ('FINISHED', 'REJECTED')
    AND r.totalSpent IS NOT NULL
GROUP BY
    Status
ORDER BY
    AvgSpend DESC;

