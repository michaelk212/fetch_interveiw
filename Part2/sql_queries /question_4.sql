SELECT
    CASE
        WHEN r.rewardsReceiptStatus = 'FINISHED' THEN 'Accepted'
        WHEN r.rewardsReceiptStatus = 'REJECTED' THEN 'Rejected'
    END AS Status,
    SUM(r.purchasedItemCount) AS TotalItemsPurchased
FROM
    receipts r
WHERE
    r.rewardsReceiptStatus IN ('FINISHED', 'REJECTED')
    AND r.purchasedItemCount IS NOT NULL
GROUP BY
    Status
ORDER BY
    TotalItemsPurchased DESC;
