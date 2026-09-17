#!/usr/bin/env bash
# Opens a tmux cockpit for the getting-started demo:
#
#   +----------------+----------------+----------------+
#   |  payment logs  | inventory logs | shipping logs  |
#   +----------------+--+-------------+----------------+
#   |  control (curl)   |       saga-server logs       |
#   +-------------------+------------------------------+
#
# The control pane starts with the README's place-order curl already typed; press Enter to run
# it and watch the saga drive the three services above. Requires the stack to be running:
#
#   docker compose up -d --wait
set -euo pipefail
cd "$(dirname "$0")"

command -v tmux >/dev/null || {
  echo "tmux is not installed (brew install tmux)" >&2
  exit 1
}
[ -n "$(docker compose ps -q saga-server 2>/dev/null)" ] || {
  echo "The stack is not running; start it first: docker compose up -d --wait" >&2
  exit 1
}

SESSION=scalardb-saga-demo
if tmux has-session -t "$SESSION" 2>/dev/null; then
  if [ -t 0 ]; then exec tmux attach -t "$SESSION"; fi
  echo "Session already exists; attach with: tmux attach -t $SESSION"
  exit 0
fi

logs='docker compose logs -f --tail 15'

# Panes are addressed by their unique ids, so this works with any base-index configuration.
payment=$(tmux new-session -d -s "$SESSION" -c "$PWD" -P -F '#{pane_id}')
control=$(tmux split-window -v -l 50% -t "$payment" -c "$PWD" -P -F '#{pane_id}')
inventory=$(tmux split-window -h -l 66% -t "$payment" -c "$PWD" -P -F '#{pane_id}')
shipping=$(tmux split-window -h -l 50% -t "$inventory" -c "$PWD" -P -F '#{pane_id}')
server=$(tmux split-window -h -l 50% -t "$control" -c "$PWD" -P -F '#{pane_id}')

tmux select-pane -t "$payment" -T payment
tmux select-pane -t "$inventory" -T inventory
tmux select-pane -t "$shipping" -T shipping
tmux select-pane -t "$control" -T control
tmux select-pane -t "$server" -T saga-server
tmux set-option -w -t "$payment" pane-border-status top

tmux send-keys -t "$payment" "$logs payment" C-m
tmux send-keys -t "$inventory" "$logs inventory" C-m
tmux send-keys -t "$shipping" "$logs shipping" C-m
tmux send-keys -t "$server" "$logs saga-server" C-m

tmux send-keys -t "$control" \
  "clear; echo 'Control pane. Press Enter to place an order, or type your own (see README.md).'" C-m
tmux send-keys -t "$control" \
  "curl -X POST localhost:12080/sagas -H 'Content-Type: application/json' -d '{\"sagaName\":\"order-saga\",\"input\":{\"orderId\":\"o-1001\",\"amount\":\"100\",\"item\":\"widget\",\"quantity\":\"2\"}}'"

tmux select-pane -t "$control"
if [ -t 0 ]; then exec tmux attach -t "$SESSION"; fi
echo "Created session '$SESSION'; attach with: tmux attach -t $SESSION"
