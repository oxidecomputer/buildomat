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

-- v 9
ALTER TABLE job_output ADD COLUMN
    time_archived   TEXT;

-- v 10
CREATE TABLE job_file (
    job             TEXT    NOT NULL,
    id              TEXT    NOT NULL,
    size            INTEGER NOT NULL,
    time_archived   TEXT,

    PRIMARY KEY (job, id)
);

-- v 11
INSERT INTO job_file (job, id, size, time_archived)
    SELECT job, id, size, time_archived FROM job_output;

-- v 12
ALTER TABLE job_output DROP COLUMN time_archived;

-- v 13
ALTER TABLE job_output DROP COLUMN size;

-- v 14
CREATE TABLE job_input (
    job             TEXT    NOT NULL,
    name            TEXT    NOT NULL,
    id              TEXT,

    PRIMARY KEY (job, name)
);

-- v 15
ALTER TABLE job ADD COLUMN
    waiting         INTEGER NOT NULL    DEFAULT 0;

-- v 16
CREATE INDEX jobs_waiting ON job (complete, waiting)
    WHERE complete = 0 AND waiting = 1;

-- v 17
CREATE INDEX jobs_active ON job (complete, waiting)
    WHERE complete = 0 AND waiting = 0;

-- v 18
CREATE TABLE job_tag (
    job             TEXT    NOT NULL,
    name            TEXT    NOT NULL,
    value           TEXT    NOT NULL,

    PRIMARY KEY (job, name)
);

-- v 19
CREATE TABLE factory (
    id              TEXT    NOT NULL    PRIMARY KEY,
    name            TEXT    NOT NULL    UNIQUE,
    token           TEXT    NOT NULL    UNIQUE,
    lastping        TEXT
);

-- v 20
ALTER TABLE worker RENAME COLUMN instance_id TO factory_private;

-- v 21
CREATE TABLE target (
    id              TEXT    NOT NULL    PRIMARY KEY,
    name            TEXT    NOT NULL    UNIQUE,
    desc            TEXT    NOT NULL,
    redirect        TEXT
);

-- v 22
INSERT INTO target (id, name, desc, redirect)
    VALUES (
        '00E82MSW0000000000000TT001',
        'helios',
        'Helios development image (helios-full-20210908-0)',
        NULL);

-- v 23
INSERT INTO target (id, name, desc, redirect)
    VALUES (
        '00E82MSW0000000000000TT000',
        'default',
        'default',
        '00E82MSW0000000000000TT001');

-- v 24
ALTER TABLE job ADD COLUMN
    target_id       TEXT;

-- v 25
ALTER TABLE worker ADD COLUMN
    factory         TEXT;

-- v 26
ALTER TABLE worker ADD COLUMN
    target          TEXT;

-- v 27
CREATE TABLE user_privilege (
    user            TEXT    NOT NULL,
    privilege       TEXT    NOT NULL,

    PRIMARY KEY (user, privilege)
);

-- v 28
ALTER TABLE target ADD COLUMN
    privilege       TEXT;

-- v 29
CREATE TABLE published_file (
    owner           TEXT    NOT NULL,
    series          TEXT    NOT NULL,
    version         TEXT    NOT NULL,
    name            TEXT    NOT NULL,
    job             TEXT    NOT NULL,
    file            TEXT    NOT NULL,

    PRIMARY KEY (owner, series, version, name)
);

-- v 30
CREATE INDEX published_file_sets ON published_file (owner, series, version);

-- v 31
ALTER TABLE worker ADD COLUMN
    wait_for_flush  INTEGER NOT NULL    DEFAULT 0;

-- v 32
ALTER TABLE job ADD COLUMN
    cancelled       INTEGER NOT NULL    DEFAULT 0;

-- v 33
CREATE TABLE job_depend (
    job             TEXT    NOT NULL,
    name            TEXT    NOT NULL,
    prior_job       TEXT    NOT NULL,
    copy_outputs    INTEGER NOT NULL,
    on_failed       INTEGER NOT NULL,
    on_completed    INTEGER NOT NULL,
    satisfied       INTEGER NOT NULL,

    PRIMARY KEY (job, name)
);

-- v 34
ALTER TABLE job_input ADD COLUMN
    other_job       TEXT;

-- v 35
CREATE TABLE job_time (
    job             TEXT    NOT NULL,
    name            TEXT    NOT NULL,
    time            TEXT    NOT NULL,

    PRIMARY KEY (job, name)
);

-- v 36
ALTER TABLE job_output_rule ADD COLUMN
    ignore          INTEGER NOT NULL    DEFAULT 0;

-- v 37
ALTER TABLE job_output_rule ADD COLUMN
    size_change_ok  INTEGER NOT NULL    DEFAULT 0;

-- v 38
ALTER TABLE job_output_rule ADD COLUMN
    require_match   INTEGER NOT NULL    DEFAULT 0;

-- v 39
CREATE TABLE job_store (
    job             TEXT    NOT NULL,
    name            TEXT    NOT NULL,
    value           TEXT    NOT NULL,
    secret          INTEGER NOT NULL,
    source          TEXT    NOT NULL,
    time_update     TEXT    NOT NULL,

    PRIMARY KEY (job, name)
);

-- v 40
CREATE INDEX workers_active ON worker (deleted) WHERE deleted = 0;

-- v 41
CREATE INDEX jobs_for_worker ON job (worker) WHERE worker IS NOT NULL;

-- v 42
ALTER TABLE worker ADD COLUMN
    factory_metadata TEXT;
