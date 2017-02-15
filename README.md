# Introduction

This allows you to dump from an postgresql in parallel using multiple threads, which can be faster than using just one. It uses one output file per thread.

Example usage:

   ./parallel_pg_select_dump -c 'postgresql://agora_elections:password@localhost:5432/agora_elections' -s '|' -q "select distinct on (voter_id) voter_id,vote from vote where election_id=1 order by voter_id asc, created desc" -n "select count(distinct voter_id) from vote where election_id=1" -d . -j 3
