alter table catalogue.job_checkpoint_lineages
    add column if not exists runtime_format text not null default 'catalogue-v1',
    add column if not exists collection_request jsonb,
    add column if not exists connector_options jsonb;

do $$
begin
    if not exists (
        select 1
          from pg_constraint
         where conrelid = 'catalogue.job_checkpoint_lineages'::regclass
           and conname = 'job_checkpoint_lineages_runtime_format_check'
    ) then
        alter table catalogue.job_checkpoint_lineages
            add constraint job_checkpoint_lineages_runtime_format_check
            check (runtime_format <> '');
    end if;

    if not exists (
        select 1
          from pg_constraint
         where conrelid = 'catalogue.job_checkpoint_lineages'::regclass
           and conname = 'job_checkpoint_lineages_library_identity_check'
    ) then
        alter table catalogue.job_checkpoint_lineages
            add constraint job_checkpoint_lineages_library_identity_check
            check (
                runtime_format <> 'commerce-scraper-v1'
                or (
                    collection_request is not null
                    and jsonb_typeof(collection_request) = 'object'
                    and connector_options is not null
                    and jsonb_typeof(connector_options) = 'object'
                )
            );
    end if;
end
$$;
