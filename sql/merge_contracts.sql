MERGE `contracts` t
USING (
    select *
    from `contracts`
    row_number() over (
    partition by
        contract_id,
        client_id,
        status
    order by
        received_at desc
    ) as the_rank
    where the_rank = 1
) s
ON s.contract_id = t.contract_id
AND s.client_id = t.client_id
AND s.status = t.status
AND s.received_at < t.received_at
WHEN MATCHED
THEN DELETE
;

-- duplicate check
-- SELECT 
-- CONCAT(contract_id, '-', client_id, status), COUNT(*)
-- FROM `contracts`
-- GROUP BY 1
-- ORDER BY 2 DESC
-- ;