#!/usr/bin/env bash
# Classifies a PR slash-command comment for slash-commands.yml.
#
# Pure decision logic (no GitHub API calls) so it can be tested locally:
#   BODY='please /testall' AUTHOR_ASSOCIATION=MEMBER \
#     ./scripts/ci_helpers/villagesql_slash_classify.sh
#
# A command is recognized ANYWHERE in the comment: the first whitespace-
# delimited token that is /<known-command> (or /help) wins, so "lgtm, /testall"
# triggers a run. Unknown slash words (e.g. /foo) are ignored, not reported.
#
# Inputs (environment variables):
#   BODY                The comment body.
#   AUTHOR_ASSOCIATION  The commenter's association (OWNER/MEMBER/COLLABORATOR/...).
#   COMMANDS            Space-separated known commands.
#                       (default: "testall testextensions")
#
# Output (key=value lines on stdout, suitable for appending to $GITHUB_OUTPUT):
#   kind=ignore        No known command found — do nothing.
#   kind=help          /help or /commands — show the command list.
#   kind=unauthorized  Known command, but commenter lacks repo access.
#   kind=run           Known command from an authorized commenter.
#   command=<name>     The command name (omitted for ignore/help).

set -euo pipefail
set -f  # no globbing while word-splitting the (untrusted) comment body

BODY="${BODY:-}"
AUTHOR_ASSOCIATION="${AUTHOR_ASSOCIATION:-}"
COMMANDS="${COMMANDS:-testall testextensions}"
ALLOWED="OWNER MEMBER COLLABORATOR"

# Scan tokens for the first known /command (or /help), anywhere in the body.
name=""
for tok in $BODY; do
  [ "${tok#/}" = "$tok" ] && continue  # not slash-prefixed
  cand=$(printf '%s' "${tok#/}" | tr '[:upper:]' '[:lower:]')
  if [ "$cand" = "help" ] || [ "$cand" = "commands" ]; then
    echo "kind=help"
    exit 0
  fi
  case " $COMMANDS " in
    *" $cand "*) name="$cand"; break ;;
  esac
done

if [ -z "$name" ]; then
  echo "kind=ignore"
  exit 0
fi

case " $ALLOWED " in
  *" $AUTHOR_ASSOCIATION "*) echo "kind=run" ;;
  *) echo "kind=unauthorized" ;;
esac
echo "command=$name"
