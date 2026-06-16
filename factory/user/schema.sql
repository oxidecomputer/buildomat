-- v 1
CREATE TABLE worker (
    id              TEXT    NOT NULL    PRIMARY KEY,
    slot            TEXT    NOT NULL,
    state           TEXT    NOT NULL,
    leased_job      TEXT    NOT NULL,
    bootstrap       TEXT    NOT NULL
);

-- v 2
CREATE INDEX worker_state ON worker (state);
