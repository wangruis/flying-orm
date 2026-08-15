create table if not exists flying_orm_batch_receipt (
    operation_id varchar(128) not null,
    chunk_index integer not null,
    plan_hash varchar(64) not null,
    payload_hash varchar(64) not null,
    row_count bigint not null,
    affected_rows bigint not null,
    status varchar(32) not null,
    created_at timestamp not null default current_timestamp,
    primary key (operation_id, chunk_index)
);
