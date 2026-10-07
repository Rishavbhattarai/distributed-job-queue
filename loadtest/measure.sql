-- Usage: psql -v run=<RUN> -f measure.sql
-- Throughput: jobs / (last attempt finished - first attempt started).
-- Enqueue-to-start: first attempt started_at - job created_at (both Postgres clocks).
WITH first AS (
    SELECT j.status, j.created_at, a.started_at, a.finished_at
      FROM jobs j
      JOIN job_attempts a ON a.job_id = j.id AND a.attempt_number = 1
     WHERE j.payload->>'run' = :'run'
)
SELECT count(*)                                                   AS jobs,
       count(*) FILTER (WHERE status = 'succeeded')               AS succeeded,
       round(extract(epoch FROM max(finished_at) - min(started_at))::numeric, 2) AS drain_s,
       round(count(*) / extract(epoch FROM max(finished_at) - min(started_at))::numeric)
                                                                  AS jobs_per_s,
       round((percentile_cont(0.5) WITHIN GROUP (
           ORDER BY extract(epoch FROM started_at - created_at)) * 1000)::numeric, 1) AS p50_ms,
       round((percentile_cont(0.99) WITHIN GROUP (
           ORDER BY extract(epoch FROM started_at - created_at)) * 1000)::numeric, 1) AS p99_ms
  FROM first;
