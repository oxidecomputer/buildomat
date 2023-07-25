#!/bin/bash

#
# Until illumos bug 13511 (svc.startd should terminate orphaned contracts for
# wait model services) is fixed, we run the agent under a noorphan contract of
# our own.  The agent's most critical job is running processes on behalf of
# user submitted jobs; we want those grandchild processes to be terminated if
# for some reason the agent exits unexpectedly.
#
exec /usr/bin/ctrun \
    -l child \
    -o noorphan,regent \
    \
    /opt/buildomat/lib/agent run
