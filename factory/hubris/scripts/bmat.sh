#!/bin/bash

#
# The agent will, when run by the hubris factory, install the "bmat" control
# program at this location:
#

exec "$HOME/.buildomat/hubris-agent/bin/bmat" "$@"
