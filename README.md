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
