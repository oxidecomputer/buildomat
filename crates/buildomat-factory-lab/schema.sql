-- v 1
CREATE TABLE instance (
    nodename        TEXT    NOT NULL,
    seq             INTEGER NOT NULL,
    worker          TEXT    NOT NULL,
    target          TEXT    NOT NULL,
    state           TEXT    NOT NULL,
    key             TEXT    NOT NULL,
    bootstrap       TEXT    NOT NULL,
    flushed         INTEGER NOT NULL,

    PRIMARY KEY (nodename, seq)
)

-- v 2
CREATE TABLE instance_event (
    nodename        TEXT    NOT NULL,
    instance        INTEGER NOT NULL,
    seq             INTEGER NOT NULL,
    stream          TEXT    NOT NULL,
    payload         TEXT    NOT NULL,
    uploaded        INTEGER NOT NULL,
    time            TEXT    NOT NULL,

    PRIMARY KEY (nodename, instance, seq)
);
