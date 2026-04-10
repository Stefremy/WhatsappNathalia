select to_regclass('public.incidencia_dispatch') as table_exists;

select indexname
from pg_indexes
where tablename = 'incidencia_dispatch';