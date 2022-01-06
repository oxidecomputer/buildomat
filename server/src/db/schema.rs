table! {
    user (id) {
        id -> Text,
        name -> Text,
        token -> Text,
        time_create -> Text,
    }
}

table! {
    job (id) {
        id -> Text,
        owner -> Text,
        name -> Text,
        target -> Text,
        complete -> Bool,
        failed -> Bool,
        worker -> Nullable<Text>,
        waiting -> Bool,
    }
}

table! {
    task (job, seq) {
        job -> Text,
        seq -> Integer,
        name -> Text,
        script -> Text,
        env_clear -> Bool,
        env -> Text,
        user_id -> Nullable<Integer>,
        group_id -> Nullable<Integer>,
        workdir -> Nullable<Text>,
        complete -> Bool,
        failed -> Bool,
    }
}

table! {
    job_input (job, name) {
        job -> Text,
        name -> Text,
        id -> Nullable<Text>,
    }
}

table! {
    job_output_rule (job, seq) {
        job -> Text,
        seq -> Integer,
        rule -> Text,
    }
}

table! {
    job_output (job, path) {
        job -> Text,
        path -> Text,
        id -> Text,
    }
}

table! {
    job_file (job, id) {
        job -> Text,
        id -> Text,
        size -> BigInt,
        time_archived -> Nullable<Text>,
    }
}

joinable!(job_file -> job (job));
allow_tables_to_appear_in_same_query!(job_file, job);

allow_tables_to_appear_in_same_query!(job_output, job_file);
allow_tables_to_appear_in_same_query!(job_input, job_file);

table! {
    job_event (job, seq) {
        job -> Text,
        task -> Nullable<Integer>,
        seq -> Integer,
        stream -> Text,
        time -> Text,
        payload -> Text,
        time_remote -> Nullable<Text>,
    }
}

table! {
    worker (id) {
        id -> Text,
        bootstrap -> Text,
        token -> Nullable<Text>,
        instance_id -> Nullable<Text>,
        deleted -> Bool,
        recycle -> Bool,
        lastping -> Nullable<Text>,
    }
}

joinable!(job -> worker (worker));
allow_tables_to_appear_in_same_query!(job, worker);
