CREATE TYPE pipeline_batch_status AS ENUM (
    'discovered',
    'downloaded',
    'loaded',
    'failed'
);
