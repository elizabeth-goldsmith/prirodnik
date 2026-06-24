#!/usr/bin/env bash
input=$(cat)

model=$(echo "$input" | jq -r '.model.display_name // empty')
cwd=$(echo "$input" | jq -r '.workspace.current_dir // empty')
used=$(echo "$input" | jq -r '.context_window.used_percentage // empty')
five=$(echo "$input" | jq -r '.rate_limits.five_hour.used_percentage // empty')
seven=$(echo "$input" | jq -r '.rate_limits.seven_day.used_percentage // empty')

out=""

if [ -n "$model" ]; then
  out="${model}"
fi

if [ -n "$cwd" ]; then
  out="${out} | ${cwd}"
fi

if [ -n "$used" ]; then
  out="${out} | ctx:$(printf '%.0f' "$used")%"
fi

limits=""
if [ -n "$five" ]; then
  limits="5h:$(printf '%.0f' "$five")%"
fi
if [ -n "$seven" ]; then
  if [ -n "$limits" ]; then
    limits="${limits} 7d:$(printf '%.0f' "$seven")%"
  else
    limits="7d:$(printf '%.0f' "$seven")%"
  fi
fi

if [ -n "$limits" ]; then
  out="${out} | ${limits}"
fi

echo "$out"