-- v 1
CREATE TABLE instance (
    id              TEXT                    PRIMARY KEY,
    worker          TEXT    NOT NULL,
    target          TEXT    NOT NULL,
    state           TEXT    NOT NULL,
    key             TEXT    NOT NULL,
    bootstrap       TEXT    NOT NULL,
    flushed         INTEGER NOT NULL
)

-- v 2
CREATE TABLE instance_event (
    instance        TEXT    NOT NULL,
    seq             INTEGER NOT NULL,
    stream          TEXT    NOT NULL,
    payload         TEXT    NOT NULL,
    uploaded        INTEGER NOT NULL,
    time            TEXT    NOT NULL,

    PRIMARY KEY (instance, seq)
);
