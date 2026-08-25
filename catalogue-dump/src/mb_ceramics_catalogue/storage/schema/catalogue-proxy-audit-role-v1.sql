-- Keep proxy audit rows immutable when the optional maintenance role has not
-- been provisioned, and authorize the invoking login rather than the owner of
-- this SECURITY DEFINER function.

create or replace function catalogue.proxy_audit_immutable() returns trigger
language plpgsql security definer
set search_path to pg_catalog, catalogue
as $$
declare
  maintenance_role oid := to_regrole('catalogue_proxy_maintenance');
begin
  if (maintenance_role is not null
      and pg_has_role(session_user, maintenance_role, 'member')
      and current_setting('catalogue.proxy_audit_maintenance', true) = 'on') is not true then
    raise exception 'proxy audit rows are immutable';
  end if;
  return old;
end;
$$;
