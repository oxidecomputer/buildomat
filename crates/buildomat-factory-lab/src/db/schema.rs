table! {
    instance (nodename, seq) {
        nodename -> Text,
        seq -> BigInt,
        worker -> Text,
        target -> Text,
        state -> Text,
        key -> Text,
        bootstrap -> Text,
        flushed -> Bool,
    }
}

table! {
    instance_event (nodename, instance, seq) {
        nodename -> Text,
        instance -> BigInt,
        seq -> BigInt,
        stream -> Text,
        payload -> Text,
        uploaded -> Bool,
        time -> Text,
    }
}
