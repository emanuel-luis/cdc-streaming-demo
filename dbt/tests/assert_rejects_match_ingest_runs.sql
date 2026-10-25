-- Every reject the consumer counted in a run row must be a row in the rejects table.
select r.n as rejects, i.n as rejected_in_runs
from (select count(*) as n from {{ ref('stg_lake__rejects') }}) as r
cross join (select coalesce(sum(rejected_rows), 0) as n from {{ ref('stg_lake__ingest_runs') }}) as i
where r.n <> i.n
