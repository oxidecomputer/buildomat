table! {
    delivery (seq) {
        seq -> BigInt,
        uuid -> Text,
        event -> Text,
        headers -> Text,
        payload -> Text,
        recvtime -> Text,
        ack -> Nullable<BigInt>,
    }
}

table! {
    repository (id) {
        id -> BigInt,
        owner -> Text,
        name -> Text,
    }
}

table! {
    check_suite (id) {
        id -> Text,
        repo -> BigInt,
        install -> BigInt,
        github_id -> BigInt,
        head_sha -> Text,
        head_branch -> Nullable<Text>,
        state -> Text,
        plan -> Nullable<Text>,
        plan_sha -> Nullable<Text>,
        url_key -> Text,
    }
}

table! {
    check_run (id) {
        id -> Text,
        check_suite -> Text,
        name -> Text,
        variety -> Text,
        content -> Nullable<Text>,
        config -> Nullable<Text>,
        private -> Nullable<Text>,
        active -> Bool,
        flushed -> Bool,
        github_id -> Nullable<BigInt>,
    }
}

joinable!(check_run -> check_suite (check_suite));
allow_tables_to_appear_in_same_query!(check_run, check_suite);
