#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: ./summarize_task.sh "Task Name" container_name

Task Name must be one of:
  - "Quality Check (Apply Changes)"
  - "Quality Check & Cleanup"          (legacy)
  - "Refresh Channel Streams"

Example:
  ./summarize_task.sh "Quality Check (Apply Changes)" dispatcharr-maid-web
USAGE
}

if [[ ${#} -ne 2 ]]; then
  usage
  exit 1
fi

task_name="$1"
container_name="$2"

case "$task_name" in
  "Quality Check (Apply Changes)"|"Quality Check & Cleanup")
    task_regex='Quality Check|Cleanup|full cleanup|cleanup only|quality check'
    action_regex='ffmpeg|ffprobe|probe|removed|remove|cleanup|duplicate|error|exception|failed'
    ;;
  "Refresh Channel Streams")
    task_regex='Refresh Channel Streams|refresh|stream refresh|refreshing'
    action_regex='refresh|fetch|scan|probe|ffmpeg|ffprobe|removed|remove|error|exception|failed'
    ;;
  *)
    echo "Unsupported task name: $task_name" >&2
    usage
    exit 1
    ;;
esac

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required but was not found in PATH." >&2
  exit 1
fi

logs=$(docker compose logs --no-color --tail 500 "$container_name" 2>&1 || true)
if [[ -z "$logs" ]]; then
  echo "No logs found for container: $container_name" >&2
  exit 1
fi

shopt -s nocasematch

start_line=""
completion_line=""
action_lines=()

while IFS= read -r line; do
  if [[ -z "$start_line" && "$line" =~ $task_regex ]]; then
    start_line="$line"
  fi

  if [[ "$line" =~ (complete|completed|finished|done) ]]; then
    completion_line="$line"
  fi

  if [[ "$line" =~ $action_regex ]]; then
    action_lines+=("$line")
  fi
done <<< "$logs"

shopt -u nocasematch

printf "Dispatcharr-Maid task summary\n"
printf "Task: %s\n" "$task_name"
printf "Container: %s\n" "$container_name"
printf "Log sample: last ~500 lines\n\n"

printf "Start (first matching log line):\n"
if [[ -n "$start_line" ]]; then
  printf "  %s\n" "$start_line"
else
  printf "  No clear start line found for this task in the last 500 lines.\n"
fi

printf "\nNotable actions (keyword matched):\n"
if [[ ${#action_lines[@]} -gt 0 ]]; then
  for line in "${action_lines[@]}"; do
    printf "  - %s\n" "$line"
  done
else
  printf "  No notable action lines matched the keyword filters.\n"
fi

printf "\nCompletion message:\n"
if [[ -n "$completion_line" ]]; then
  printf "  %s\n" "$completion_line"
else
  printf "  No completion message found in the last 500 lines.\n"
fi

printf "\nNote: This summary reports only what appears in the logs; it does not infer success or failure.\n"
