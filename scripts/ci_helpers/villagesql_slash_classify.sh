#!/usr/bin/env bash
# Classifies a PR slash-command comment for slash-commands.yml.
#
# Pure decision logic (no GitHub API calls) so it can be tested locally:
#   BODY='/testall' AUTHOR_ASSOCIATION=MEMBER ./scripts/villagesql_slash_classify.sh
#
# Inputs (environment variables):
#   BODY                The comment body.
#   AUTHOR_ASSOCIATION  The commenter's association (OWNER/MEMBER/COLLABORATOR/...).
#   COMMANDS            Space-separated known commands.
#                       (default: "testall testextensions")
#
# Output (key=value lines on stdout, suitable for appending to $GITHUB_OUTPUT):
#   kind=ignore        Not a slash command — do nothing.
#   kind=help          /help or /commands — show the command list.
#   kind=unknown       A /command we don't recognize.
#   kind=unauthorized  Known command, but commenter lacks repo access.
#   kind=run           Known command from an authorized commenter.
#   command=<name>     The command name (omitted for ignore/help).

set -euo pipefail

BODY="${BODY:-}"
AUTHOR_ASSOCIATION="${AUTHOR_ASSOCIATION:-}"
COMMANDS="${COMMANDS:-testall testextensions}"
ALLOWED="OWNER MEMBER COLLABORATOR"

# First whitespace-delimited token of the comment.
first_tok=$(printf '%s' "$BODY" | awk 'NF {print $1; exit}')

if [ "${first_tok#/}" = "$first_tok" ]; then
  echo "kind=ignore"
  exit 0
fi

# Strip leading slash, lowercase (tr for bash 3.2 / macOS portability).
name=$(printf '%s' "${first_tok#/}" | tr '[:upper:]' '[:lower:]')

if [ "$name" = "help" ] || [ "$name" = "commands" ]; then
  echo "kind=help"
  exit 0
fi

case " $COMMANDS " in
  *" $name "*) ;;
  *)
    echo "kind=unknown"
    echo "command=$name"
    exit 0
    ;;
esac

case " $ALLOWED " in
  *" $AUTHOR_ASSOCIATION "*)
    echo "kind=run"
    ;;
  *)
    echo "kind=unauthorized"
    ;;
esac
echo "command=$name"
