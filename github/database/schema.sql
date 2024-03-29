-- v 1
CREATE TABLE delivery (
    seq             INTEGER             PRIMARY KEY,
    uuid            TEXT    NOT NULL    UNIQUE,
    event           TEXT    NOT NULL,
    headers         TEXT    NOT NULL,
    payload         TEXT    NOT NULL,
    recvtime        TEXT    NOT NULL,
    ack             INTEGER
);

-- v 2
CREATE TABLE repository (
    id              INTEGER             PRIMARY KEY,
    owner           TEXT    NOT NULL,
    name            TEXT    NOT NULL
);

-- v 3
CREATE TABLE check_suite (
    id              TEXT                PRIMARY KEY,
    repo            INTEGER NOT NULL,
    install         INTEGER NOT NULL,
    github_id       INTEGER NOT NULL,
    head_sha        TEXT    NOT NULL,
    head_branch     TEXT,
    state           TEXT    NOT NULL,
    plan            TEXT,
    plan_sha        TEXT,
    url_key         TEXT    NOT NULL,

    UNIQUE (repo, github_id)
);

-- v 4
CREATE TABLE check_run (
    id              TEXT                PRIMARY KEY,
    check_suite     TEXT    NOT NULL    REFERENCES check_suite (id)
                                            ON UPDATE RESTRICT
                                            ON DELETE RESTRICT,
    name            TEXT    NOT NULL,
    variety         TEXT    NOT NULL,
    content         TEXT,
    config          TEXT,
    private         TEXT,
    active          INTEGER NOT NULL,
    flushed         INTEGER NOT NULL,
    github_id       INTEGER
);

-- v 5
CREATE INDEX delivery_ack ON delivery (ack);

-- v 6
CREATE TABLE install (
    id              INTEGER             PRIMARY KEY,
    owner           INTEGER NOT NULL
);

-- v 7
CREATE TABLE user (
    id              INTEGER             PRIMARY KEY,
    login           TEXT    NOT NULL,
    usertype        TEXT    NOT NULL,
    name            TEXT,
    email           TEXT
);

-- v 8
ALTER TABLE check_suite ADD COLUMN
    pr_by           INTEGER;

-- v 9
ALTER TABLE check_suite ADD COLUMN
    requested_by    INTEGER;

-- v 10
ALTER TABLE check_suite ADD COLUMN
    approved_by     INTEGER;

-- v 11
CREATE INDEX repository_owner_name ON repository (owner, name);

-- v 12
ALTER TABLE check_run ADD COLUMN
    dependencies    TEXT;

-- v 13
CREATE INDEX check_run_check_suite ON check_run (check_suite);
