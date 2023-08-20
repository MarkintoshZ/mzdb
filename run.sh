tmux send-keys -t 1 'RUST_LOG=debug ./target/debug/mzdb "[::]:8000" "[::]:8001" 21' Enter
tmux send-keys -t 2 'RUST_LOG=debug ./target/debug/mzdb "[::]:8001" "[::]:8000" 86' Enter
