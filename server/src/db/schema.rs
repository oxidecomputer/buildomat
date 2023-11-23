use sea_query::Iden;

#[derive(Iden)]
pub enum User {
    Table,
    Id,
    Name,
    Token,
    TimeCreate,
}

#[derive(Iden)]
pub enum Job {
    Table,
    Id,
    Owner,
    Name,
    Target,
    Complete,
    Failed,
    Worker,
    Waiting,
    TargetId,
    Cancelled,
    TimeArchived,
}

#[derive(Iden)]
pub enum JobTag {
    Table,
    Job,
    Name,
    Value,
}

#[derive(Iden)]
pub enum Task {
    Table,
    Seq,
    Name,
    Script,
    EnvClear,
    Env,
    UserId,
    GroupId,
    Workdir,
    Complete,
    Failed,
}

#[derive(Iden)]
pub enum JobInput {
    Table,
    Job,
    Seq,
    Rule,
    Ignore,
    SizeChangeOk,
    RequireMatch,
}

#[derive(Iden)]
pub enum JobOutput {
    Table,
    Job,
    Path,
    Id,
}

#[derive(Iden)]
pub enum JobFile {
    Table,
    Job,
    Id,
    Size,
    TimeArchived,
}

#[derive(Iden)]
pub enum JobEvent {
    Table,
    Job,
    Task,
    Seq,
    Stream,
    Time,
    Payload,
    TimeRemote,
}

#[derive(Iden)]
pub enum Worker {
    Table,
    Id,
    Bootstrap,
    Token,
    FactoryPrivate,
    Deleted,
    Recycle,
    Lastping,
    Factory,
    Target,
    WaitForFlush,
    FactoryMetadata,
}

#[derive(Iden)]
pub enum Factory {
    Table,
    Id,
}

#[derive(Iden)]
#[derive(Iden)]
#[derive(Iden)]
#[derive(Iden)]
#[derive(Iden)]
