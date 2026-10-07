# Chaos scripts (Week 2 / Week 4)

`kill_random_worker.sh` will `kill -9` a random worker container every 10 seconds during a
load run. Success = zero lost jobs. This needs leases + reaper (Week 2) to be meaningful.
