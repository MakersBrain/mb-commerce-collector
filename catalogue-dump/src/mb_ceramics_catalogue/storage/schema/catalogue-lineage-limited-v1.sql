alter table catalogue.job_checkpoint_lineages
    drop constraint if exists job_checkpoint_lineages_status_check,
    drop constraint if exists job_checkpoint_lineages_check1;

alter table catalogue.job_checkpoint_lineages
    add constraint job_checkpoint_lineages_status_check
        check (status = any (array[
            'active'::text,
            'completed'::text,
            'limited'::text,
            'rejected'::text,
            'expired'::text
        ])),
    add constraint job_checkpoint_lineages_check1
        check (status not in ('completed', 'limited') or checksum is not null);
