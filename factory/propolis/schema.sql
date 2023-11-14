-- v 1
CREATE TABLE instance (
    nodename        TEXT    NOT NULL,
    seq             INTEGER NOT NULL,
    worker          TEXT    NOT NULL,
    lease           TEXT    NOT NULL,
    target          TEXT    NOT NULL,
    state           TEXT    NOT NULL,
    bootstrap       TEXT    NOT NULL,
    slot            INTEGER NOT NULL,

    PRIMARY KEY (nodename, seq)
)

-- v 2
CREATE INDEX instance_active ON instance (state)
    WHERE state <> 'destroyed';
