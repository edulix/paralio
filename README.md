# Introduction

[![Build Status](https://travis-ci.org/edulix/paralio.svg?branch=master)](https://travis-ci.org/edulix/paralio)

This repository contains some tools that execute some specific tasks in parallel, with the performance advantages this might have depending on the scenario.

## ppdump

ppdump executes a select SQL query in a PostgreSQL database in parallel, where each thread returns a batch of the result. It works by executign the query multiple times at once in different threads, with the query modified in each execution with different limits and offsets, so that each execution delivers one part of the results. This can be faster than using just one thread because of the current limitations of PostgreSQL. It uses one output file per thread.

ppdump will probably not be so useful in future versions of PostgreSQL thanks to its increasing support for parallel queries, but it would still be useful in some uses-cases where an older version of PostgreSQL is used or in queries that do not allow Parallel queries execution yet.

### Example usage

    ppdump\
      -c 'postgresql://agora_elections:password@localhost:5432/agora_elections'\
      -s '|'\
      -q "select distinct on (voter_id) voter_id,vote from vote where election_id=1 order by voter_id asc, created desc"\
      -n "select count(distinct voter_id) from vote where election_id=1"\
      -d .\
      -j 3

## pjoin

pjoin join lines of two files on a common field. It works very much like the join linux command but:
1. applies thread-level parallelism to the speed-up the join task.
2. Allows each input files to be provided as a list of ordered files, so that it can process directly the output for example of ppdump.

pjoin:
- uses one output file per thread. 
- asumes both input files are ordered and formated with each element inside each file correspond with new-line-separated lines. 
- the technical term for the kind of parallelization algorithm that executes the join is a "skew partition parallel join".

### Example usage

    pjoin\
      -a ~/pjoin_ramdisk/all_sorted_ballots\
      -b ~/pjoin_ramdisk/all_sorted_voterids\
      -s '|'\
      -f 1.1\
      -o ~/pjoin_ramdisk/output\
      -j 128

# TODO: Some unorganized documentation

## local

cd ~/proyectos/agora/paralio
cargo build --release
scp target/release/pjoin dump_generator.py aws-ops:~/

## aws-ops

scp ~/pjoin ~/dump_generator.py test-vm:~/

## ubuntu@aeci-s1

rm -rf ~/pjoin_testdir
mkdir ~/pjoin_testdir

time python3 ~/dump_generator.py --add-ballot 1000000 > ~/pjoin_testdir/all_ballots

real    0m34.359s
user    0m28.096s
sys     0m5.653s

time wc -l ~/pjoin_testdir/all_ballots
1000000 /home/ubuntu/pjoin_testdir/all_ballots

real    0m1.189s
user    0m0.208s
sys     0m0.981s

time cut -d '|' -f 1 ~/pjoin_testdir/all_ballots > ~/pjoin_testdir/all_voterids
time sort -t '|' -k 1 ~/pjoin_testdir/all_voterids > ~/pjoin_testdir/all_sorted_voterids
time sort -t '|' -k 1 ~/pjoin_testdir/all_ballots > ~/pjoin_testdir/all_sorted_ballots


time  join --nocheck-order ~/pjoin_testdir/all_sorted_ballots ~/pjoin_testdir/all_sorted_voterids -t '|' -o 1.2 > ~/pjoin_testdir/join_all_sorted_filtered_ballots
wc -l ~/pjoin_testdir/join_all_sorted_filtered_ballots
time md5sum ~/pjoin_testdir/join_all_sorted_filtered_ballots
08f700bec5097951dd343c63c505b4dc

mkdir ~/pjoin_ramdisk

# use join on ram

sudo umount ~/pjoin_ramdisk
sudo mount -t tmpfs -o size=25G tmpfs ~/pjoin_ramdisk

time cp ~/pjoin_testdir/all_sorted_ballots ~/pjoin_testdir/all_sorted_voterids ~/pjoin_ramdisk
sync
sudo sh -c 'echo 3 >/proc/sys/vm/drop_caches'
time  join --nocheck-order ~/pjoin_ramdisk/all_sorted_ballots ~/pjoin_ramdisk/all_sorted_voterids -t '|' -o 1.2 > ~/pjoin_ramdisk/join_all_sorted_filtered_ballots

# real    0m21.170s
# user    0m17.308s
# sys     0m3.860s

wc -l ~/pjoin_ramdisk/join_all_sorted_filtered_ballots
# 1000000 /home/ubuntu/pjoin_ramdisk/join_all_sorted_filtered_ballots
ls -lah ~/pjoin_ramdisk/join_all_sorted_filtered_ballots
# -rw-rw-r-- 1 ubuntu ubuntu 7.4G Feb 26 08:38 /home/ubuntu/pjoin_ramdisk/join_all_sorted_filtered_ballots
md5sum ~/pjoin_ramdisk/join_all_sorted_filtered_ballots
# 08f700bec5097951dd343c63c505b4dc  /home/ubuntu/pjoin_ramdisk/join_all_sorted_filtered_ballots


# use pjoin on ram, 1 job

sudo umount ~/pjoin_ramdisk
sudo mount -t tmpfs -o size=25G tmpfs ~/pjoin_ramdisk

time cp ~/pjoin_testdir/all_sorted_ballots ~/pjoin_testdir/all_sorted_voterids ~/pjoin_ramdisk
mkdir ~/pjoin_ramdisk/output
sync
sudo sh -c 'echo 3 >/proc/sys/vm/drop_caches'
time ./pjoin -a ~/pjoin_ramdisk/all_sorted_ballots -b ~/pjoin_ramdisk/all_sorted_voterids -s '|' -f 1.1 -o ~/pjoin_ramdisk/output -j 1

# real    0m14.518s
# user    0m10.372s
# sys     0m4.108s

wc -l ~/pjoin_ramdisk/output/0
# 1000000 /home/ubuntu/pjoin_ramdisk/output/0
ls -lah ~/pjoin_ramdisk/output/0
# -rw-rw-r-- 1 ubuntu ubuntu 7.4G Feb 26 08:42 /home/ubuntu/pjoin_ramdisk/output/0
md5sum ~/pjoin_ramdisk/output/0
# 08f700bec5097951dd343c63c505b4dc

# use pjoin on ram

sudo umount ~/pjoin_ramdisk
sudo mount -t tmpfs -o size=25G tmpfs ~/pjoin_ramdisk

time cp ~/pjoin_testdir/all_sorted_ballots ~/pjoin_testdir/all_sorted_voterids ~/pjoin_ramdisk
mkdir ~/pjoin_ramdisk/output
sync
sudo sh -c 'echo 3 >/proc/sys/vm/drop_caches'
time ./pjoin -a ~/pjoin_ramdisk/all_sorted_ballots -b ~/pjoin_ramdisk/all_sorted_voterids -s '|' -f 1.1 -o ~/pjoin_ramdisk/output -j 128

## 36.1
thread=0 elapsed=0s 1.525.275ns
thread=1 elapsed=0s 3.038.262ns

# 128.2

thread=127 elapsed=1s 349.389.798ns
thread=127 END elapsed=1s 501.440.661ns

# real    0m2.428s
# user    0m13.360s
# sys     0m5.020s

wc -l ~/pjoin_ramdisk/output/*
#  100001 /home/ubuntu/pjoin_ramdisk/output/0
#  100000 /home/ubuntu/pjoin_ramdisk/output/1
#  100000 /home/ubuntu/pjoin_ramdisk/output/2
#  100000 /home/ubuntu/pjoin_ramdisk/output/3
#  100000 /home/ubuntu/pjoin_ramdisk/output/4
#  100000 /home/ubuntu/pjoin_ramdisk/output/5
#  100000 /home/ubuntu/pjoin_ramdisk/output/6
#  100000 /home/ubuntu/pjoin_ramdisk/output/7
#  100000 /home/ubuntu/pjoin_ramdisk/output/8
#   99999 /home/ubuntu/pjoin_ramdisk/output/9
# 1000000 total
ls -lah ~/pjoin_ramdisk/output/*
# -rw-rw-r-- 1 ubuntu ubuntu 756M Feb 26 08:45 /home/ubuntu/pjoin_ramdisk/output/0
# -rw-rw-r-- 1 ubuntu ubuntu 756M Feb 26 08:45 /home/ubuntu/pjoin_ramdisk/output/1
# -rw-rw-r-- 1 ubuntu ubuntu 756M Feb 26 08:45 /home/ubuntu/pjoin_ramdisk/output/2
# -rw-rw-r-- 1 ubuntu ubuntu 756M Feb 26 08:45 /home/ubuntu/pjoin_ramdisk/output/3
# -rw-rw-r-- 1 ubuntu ubuntu 756M Feb 26 08:45 /home/ubuntu/pjoin_ramdisk/output/4
# -rw-rw-r-- 1 ubuntu ubuntu 756M Feb 26 08:45 /home/ubuntu/pjoin_ramdisk/output/5
# -rw-rw-r-- 1 ubuntu ubuntu 756M Feb 26 08:45 /home/ubuntu/pjoin_ramdisk/output/6
# -rw-rw-r-- 1 ubuntu ubuntu 756M Feb 26 08:45 /home/ubuntu/pjoin_ramdisk/output/7
# -rw-rw-r-- 1 ubuntu ubuntu 756M Feb 26 08:45 /home/ubuntu/pjoin_ramdisk/output/8
# -rw-rw-r-- 1 ubuntu ubuntu 756M Feb 26 08:45 /home/ubuntu/pjoin_ramdisk/output/9
du -sh ~/pjoin_ramdisk/output/
# 7.4G    /home/ubuntu/pjoin_ramdisk/output/


# local debug

rm -rf ~/pjoin_testdir
mkdir -p ~/pjoin_testdir/output

python3 ./dump_generator.py --add-ballot 1000000 > ~/pjoin_testdir/all_ballots
cut -d '|' -f 1 ~/pjoin_testdir/all_ballots > ~/pjoin_testdir/all_voterids
sort -t '|' -k 1 ~/pjoin_testdir/all_voterids > ~/pjoin_testdir/all_sorted_voterids
sort -t '|' -k 1 ~/pjoin_testdir/all_ballots > ~/pjoin_testdir/all_sorted_ballots

clear; cargo run --bin pjoin -- -a ~/pjoin_testdir/all_sorted_ballots -b ~/pjoin_testdir/all_sorted_voterids -s '|' -f 1.1 -o ~/pjoin_testdir/output -j 2

wc -l ~/pjoin_testdir/output/*

# Improvements

- generate the B slices in the main thread
- generate a lazy search index to reduce disk lookups
- use a interpol search or similar
- http://blog.teamleadnet.com/2014/06/beating-binary-search-algorithm.html
- Reduce buffer size and allow reading lines bigger than buffer size
