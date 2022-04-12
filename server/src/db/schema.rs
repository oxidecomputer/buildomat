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
        target_id -> Nullable<Text>,
        cancelled -> Bool,
    }
}

table! {
    job_tag (job, name) {
        job -> Text,
        name -> Text,
        value -> Text,
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
        factory_private -> Nullable<Text>,
        deleted -> Bool,
        recycle -> Bool,
        lastping -> Nullable<Text>,
        factory -> Nullable<Text>,
        target -> Nullable<Text>,
        wait_for_flush -> Bool,
    }
}

joinable!(job -> worker (worker));
allow_tables_to_appear_in_same_query!(job, worker);

table! {
    factory (id) {
        id -> Text,
        name -> Text,
        token -> Text,
        lastping -> Nullable<Text>,
    }
}

table! {
    target (id) {
        id -> Text,
        name -> Text,
        desc -> Text,
        redirect -> Nullable<Text>,
        privilege -> Nullable<Text>,
    }
}

table! {
    user_privilege (user, privilege) {
        user -> Text,
        privilege -> Text,
    }
}

table! {
    published_file (owner, series, version, name) {
        owner -> Text,
        series -> Text,
        version -> Text,
        name -> Text,
        job -> Text,
        file -> Text,
    }
}

allow_tables_to_appear_in_same_query!(published_file, job_file);
