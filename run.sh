trap 'kill %1' SIGINT
./target/debug/mzdb [::]:8000 [::]:8001 21 & \
./target/debug/mzdb [::]:8001 [::]:8000 86
