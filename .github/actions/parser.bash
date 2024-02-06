#!/usr/bin/env bash
parsePR () {
variable=$1
value=$(echo "$PR_BODY" | grep "<!-- $variable -->" - | cut -d'[' -f2 | cut -d']' -f1)
 if [ "$value" == 'x' ];
  then result="true";
  else result="false"; fi
echo "$variable"="$result" >> "$GITHUB_ENV"
}
