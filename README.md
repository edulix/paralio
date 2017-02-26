# Introduction

This allows you to dump from an postgresql in parallel using multiple threads, which can be faster than using just one. It uses one output file per thread.

Example usage:

./parallel_pg_select_dump -c 'postgresql://agora_elections:password@localhost:5432/agora_elections' -s '|' -q "select distinct on (voter_id) voter_id,vote from vote where election_id=1 order by voter_id asc, created desc" -n "select count(distinct voter_id) from vote where election_id=1" -d . -j 3

## local

cd ~/proyectos/agora/parallel_pg_select_dump
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
time  join --nocheck-order ~/pjoin_ramdisk/all_sorted_ballots ~/pjoin_ramdisk/all_sorted_voterids -t '|' -o 1.2 > ~/pjoin_ramdisk/join_all_sorted_filtered_ballots

real    0m21.170s
user    0m17.308s
sys     0m3.860s

# use pjoin on ram, 1 job

sudo umount ~/pjoin_ramdisk
sudo mount -t tmpfs -o size=1G tmpfs ~/pjoin_ramdisk

time cp ~/pjoin_testdir/all_sorted_ballots ~/pjoin_testdir/all_sorted_voterids ~/pjoin_ramdisk
mkdir ~/pjoin_ramdisk/output
time ./pjoin -a ~/pjoin_ramdisk/all_sorted_ballots -b ~/pjoin_ramdisk/all_sorted_voterids -s '|' -f 1.1 -o ~/pjoin_ramdisk/output -j 1

real    0m14.738s
user    0m10.436s
sys     0m4.296s

time md5sum ~/pjoin_ramdisk/output/0
08f700bec5097951dd343c63c505b4dc

# use pjoin on ram, 2 jobs


sudo umount ~/pjoin_ramdisk
sudo mount -t tmpfs -o size=25G tmpfs ~/pjoin_ramdisk

time cp ~/pjoin_testdir/all_sorted_ballots ~/pjoin_testdir/all_sorted_voterids ~/pjoin_ramdisk
mkdir ~/pjoin_ramdisk/output
time ./pjoin -a ~/pjoin_ramdisk/all_sorted_ballots -b ~/pjoin_ramdisk/all_sorted_voterids -s '|' -f 1.0 -o ~/pjoin_ramdisk/output -j 2

wc -l ~/pjoin_ramdisk/output/*


# local debug

rm -rf ~/pjoin_testdir
mkdir -p ~/pjoin_testdir/output

python3 ./dump_generator.py --add-ballot 10000 > ~/pjoin_testdir/all_ballots
cut -d '|' -f 1 ~/pjoin_testdir/all_ballots > ~/pjoin_testdir/all_voterids
sort -t '|' -k 1 ~/pjoin_testdir/all_voterids > ~/pjoin_testdir/all_sorted_voterids
sort -t '|' -k 1 ~/pjoin_testdir/all_ballots > ~/pjoin_testdir/all_sorted_ballots

clear; cargo run --bin pjoin -- -a ~/pjoin_testdir/all_sorted_ballots -b ~/pjoin_testdir/all_sorted_voterids -s '|' -f 1.1 -o ~/pjoin_testdir/output -j 2

wc -l ~/pjoin_testdir/output/*

