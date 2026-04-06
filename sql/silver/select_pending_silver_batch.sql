WITH candidate AS (
    SELECT pb.batch_id
    FROM pipeline_batches pb
    WHERE pb.file_type = 'export'
        AND pb.status = 'loaded'
        AND (
            pb.claimed_at IS NULL
            OR pb.claimed_at < NOW() - (%(claim_ttl_minutes)s * INTERVAL '1 minute')
        )
        AND EXISTS (
            SELECT 1
            FROM events_bronze eb
            WHERE eb.batch_id = pb.batch_id
        )
        AND NOT EXISTS (
            SELECT 1
            FROM events_silver es
            WHERE es.batch_id = pb.batch_id
        )
    ORDER BY pb.gdelt_timestamp ASC
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE pipeline_batches pb
SET
    claimed_at = NOW(),
    claimed_by = %(claimed_by)s,
    error_message = NULL
FROM candidate
WHERE pb.batch_id = candidate.batch_id
RETURNING
    pb.batch_id,
    pb.source_type,
    pb.file_type,
    pb.source_url,
    pb.file_name,
    pb.gdelt_timestamp,
    pb.status,
    pb.claimed_at,
    pb.claimed_by;
