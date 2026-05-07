CREATE TYPE pipeline_batch_status AS ENUM (
    'discovered',
    'downloaded',
    'uploaded',
    'loaded',
    'failed'
);
