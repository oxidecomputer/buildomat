-- v 1
CREATE TABLE user (
    id              TEXT                PRIMARY KEY,
    name            TEXT    NOT NULL    UNIQUE,
    token           TEXT    NOT NULL    UNIQUE,
    time_create     TEXT    NOT NULL
);

-- v 2
CREATE TABLE job (
    id              TEXT                PRIMARY KEY,
    owner           TEXT    NOT NULL,
    name            TEXT    NOT NULL,
    target          TEXT    NOT NULL,
    complete        INTEGER NOT NULL,
    failed          INTEGER NOT NULL,
    worker          TEXT
);

-- v 3
CREATE TABLE task (
    job             TEXT    NOT NULL,
    seq             INTEGER NOT NULL,
    name            TEXT    NOT NULL,
    script          TEXT    NOT NULL,
    env_clear       INTEGER NOT NULL,
    env             TEXT    NOT NULL,
    user_id         INTEGER,
    group_id        INTEGER,
    workdir         TEXT,
    complete        INTEGER NOT NULL,
    failed          INTEGER NOT NULL,

    PRIMARY KEY (job, seq)
);

-- v 4
CREATE TABLE job_output_rule (
    job             TEXT    NOT NULL,
    seq             INTEGER NOT NULL,
    rule            TEXT    NOT NULL,

    PRIMARY KEY (job, seq)
);

-- v 5
CREATE TABLE job_output (
    job             TEXT    NOT NULL,
    path            TEXT    NOT NULL,
    size            INTEGER NOT NULL,
    id              TEXT    NOT NULL    UNIQUE,

    PRIMARY KEY (job, path)
);

-- v 6
CREATE TABLE job_event (
    job             TEXT    NOT NULL,
    task            INTEGER,
    seq             INTEGER NOT NULL,
    stream          TEXT    NOT NULL,
    time            TEXT    NOT NULL,
    payload         TEXT    NOT NULL,

    PRIMARY KEY (job, seq)
);

-- v 7
CREATE TABLE worker (
    id              TEXT                PRIMARY KEY,
    bootstrap       TEXT    NOT NULL    UNIQUE,
    token           TEXT                UNIQUE,
    instance_id     TEXT,
    deleted         INTEGER,
    recycle         INTEGER,
    lastping        TEXT
);

-- v 8
ALTER TABLE job_event ADD COLUMN
    time_remote     TEXT;
