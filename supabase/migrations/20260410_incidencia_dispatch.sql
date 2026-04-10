create table if not exists incidencia_dispatch (
  id bigserial primary key,
  incidencia_id bigint not null,
  notification_type text not null,
  day_key date not null,
  status text not null default 'claimed', -- claimed|sent|failed
  claimed_at timestamptz not null default now(),
  sent_at timestamptz,
  error text
);

create unique index if not exists uq_incidencia_dispatch
  on incidencia_dispatch (incidencia_id, notification_type, day_key);

create index if not exists ix_incidencia_dispatch_status
  on incidencia_dispatch (status, claimed_at);
