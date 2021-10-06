#!/bin/bash

#
# Regrettably there is no simple file- or API-based mechanism for configuring a
# GitHub App.  One can arrange for a page with a form that will POST something
# to GitHub, but that requires a server for a callback URL, which is remarkably
# tedious for something that should be a simple PUT or POST with appropriate
# credentials.
#
# The alternative is to specify a set of query parameters for the regular
# manual creation page, which will pre-populate some (but of course, not all!)
# of the form.  The operator is then expected to copy and paste additional
# things like secrets and private keys.  In keeping with this distasteful
# second option, we will produce the required URL, distastefully, via this
# shell script.
#

if [[ -n $1 ]]; then
	#
	# The URL is different if you want to create the App in an
	# Organisation.
	#
	url="https://github.com/organizations/$1/settings/apps/new?"
else
	url='https://github.com/settings/apps/new?'
fi

first=yes
function add {
	local name=$1
	shift
	local value="$*"

	#
	# Escape!
	#
	value="${value// /+}"

	if [[ $first == no ]]; then
		url+='&'
	fi
	first=no

	url+="$name=$value"
}

add 'name'		'buildomat'
add 'description'	'a software build labour saving device'
add 'url'		'https://buildomat.eng.oxide.computer'
add 'public'		'true'
add 'webhook_url'	'https://buildomat.eng.oxide.computer/wg/0/webhook'
add 'events[]'		'check_run'
add 'events[]'		'check_suite'
add 'events[]'		'create'
add 'events[]'		'delete'
add 'events[]'		'public'
add 'events[]'		'pull_request'
add 'events[]'		'push'
add 'events[]'		'repository'

#
# Permissions:
#
add 'checks'		'write'
add 'contents'		'read'
add 'metadata'		'read'
add 'pull_requests'	'read'

printf '%s\n' "$url"
