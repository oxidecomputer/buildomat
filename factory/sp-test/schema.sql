-- v 1
CREATE TABLE instance (
    testbed_name    TEXT    NOT NULL,
    seq             INTEGER NOT NULL,
    worker          TEXT    NOT NULL,
    target          TEXT    NOT NULL,
    state           TEXT    NOT NULL,
    bootstrap       TEXT    NOT NULL,
    flushed         INTEGER NOT NULL,

    PRIMARY KEY (testbed_name, seq)
)

-- v 2
CREATE INDEX instance_active ON instance (state)
    WHERE state NOT IN ('destroyed')
