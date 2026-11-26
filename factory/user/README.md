# User Factory

The user factory creates unprivileged users on a shared illumos machine to run
jobs.  The factory ensures each job runs in an ephemeral system user with no
privileges and no ability to write files outside of allowlisted directories, and
cleans up any leftover files at the end of the job.

It currently only supports running on illumos, and requires that the factory
runs with the `Primary Administrator` profile.

## Slots 

The factory configuration has to define one or more named "slots".  Each slot
has a buildomat target it supports, plus optional configuration to define its
execution environment.  When the a job comes in from the buildomat core server,
the factory will only accept it if there is a free slot for that job's target.
This means the number of slots is also the concurrency limit on that system.

The minimal slot configuration requires only a slot name and the ID of the
buildomat target the slot supports:

```toml
[slots.NAME]
target = "TARGET_ID"
```

Additional configuration options are available:

* **`add_to_groups = ["GROUP", "GROUP"]`**: add the ephemeral system user to
  these groups in addition to the primary (ephemeral) group.  This can be useful
  to allow jobs running in the slot to access system capabilities guarded by
  group membership.

* **`conflicts_with_slots = ["SLOT", "SLOT"]`**: prevent a job from running in
  this slot if any job is running in one of the configured conflicting jobs.
  The reverse also applies: a job running in a conflicting slot will prevent a
  job from running in the current slot.

* **`env.KEY = "VALUE"`**: set arbitrary environment variables in jobs executed
  inside of this slot.

An example of a full slot configuration:

```toml
[slots.hubris-1]
target = "..."
add_to_groups = ["usb"]
conflicts_with_slots = ["hubris-2"]
env.HUMILITY_ENVIRONMENT = "/opt/ci/humility/1.json"
```

## Isolation

Each job runs as an ephemeral user, with randomly generated UID and GID,
guaranteed not to collide with any UID and GID currently in use on the system.
The name of the user is the worker ID prefixed by `bmat-`.

The whole filesystem is enforced to be read-only, except for a few allowlisted
directories. All writeable directories are empty at the time the job starts, and
are purged at the end of the job. The following directiories are writeable:

* `/home/$USER`
* `/input`
* `/opt/buildomat` (used by the buildomat agent)
* `/tmp`
* `/var/run` (used by the buildomat agent)
* `/var/tmp`
* `/work`
