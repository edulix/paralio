/**
 * Copyright (C) 2017 Eduardo Robles Elvira <edulix@nvotes.com>

 * parallel_pg_select_dump is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License.

 * parallel_pg_select_dump  is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.

 * You should have received a copy of the GNU Affero General Public License
 * along with parallel_pg_select_dump.  If not, see <http://www.gnu.org/licenses/>.
**/

extern crate postgres;
extern crate time;

#[macro_use]
extern crate clap;

use clap::App;
use std::thread;
use std::time::Instant;
use std::io::prelude::*;
use std::fs::File;
use std::path::Path;
use std::io::BufWriter;
use std::fmt::Write as StdWrite;

use postgres::{Connection, TlsMode};

fn main()
{
  let yaml = load_yaml!("cli.yml");
  let matches = App::from_yaml(yaml).get_matches();

  let query_str = matches.value_of("query").unwrap().to_string();
  let count_query_str = matches.value_of("count-query").unwrap().to_string();
  let connection_str = matches.value_of("connection").unwrap().to_string();
  let directory = matches.value_of("directory").unwrap().to_string();
  let separator = matches.value_of("separator").unwrap().to_string();
  let batch_size: i64 = matches.value_of("batch-size").unwrap().parse().unwrap();
  let verbose: bool = matches.is_present("verbose");

  let njobs: i32 = matches.value_of("jobs").unwrap().parse().unwrap();
  let mut children = vec![];

  let start = Instant::now();

  if verbose {
    println!("{} secs \tbase_thread: counting rows with sentence: {}",
      start.elapsed().as_secs(), count_query_str
    );
  }
  let conn = Connection::connect(connection_str.as_str(), TlsMode::None)
    .unwrap();
  let stmt = conn.prepare(&count_query_str).unwrap();
  let total_count: i64 = stmt.query(&[]).unwrap().get(0).get(0);
  let thread_size: i64 = (total_count + njobs as i64) / njobs as i64;
  let num_batches: i64 = (thread_size as f64 / batch_size as f64).ceil() as i64;

  if verbose {
    println!("{} secs \tbase_thread: ... counted: {} total, {} thread_size for {} jobs",
      start.elapsed().as_secs(), total_count, thread_size, njobs
    );
  }

  for thread_num in 0..njobs
  {
    let query_str = query_str.clone();
    let connection_str = connection_str.clone();
    let directory = directory.clone();
    let separator = separator.clone();
    let batch_size = batch_size.clone();
    let thread_size = thread_size.clone();
    let verbose = verbose.clone();

    children.push( thread::spawn(move || {

        if verbose {
          println!("{} secs \tthread {}: starting", start.elapsed().as_secs(),
            thread_num
          );
        }
        let conn = Connection::connect(
          connection_str.as_str(), TlsMode::None
        ).unwrap();

        let mut limited_query_str = String::new();
        write!(&mut limited_query_str, "{} limit $1 offset $2", query_str).unwrap();
        let stmt = conn.prepare(&limited_query_str).unwrap();
        let path = Path::new(&directory).join(thread_num.to_string());
        let mut file = BufWriter::new(File::create(path).unwrap());

        for batch_num in 0..num_batches
        {
          let offset = thread_num as i64 * thread_size + batch_num * batch_size;
          let batch_size = if batch_num == num_batches - 1 {
            thread_size - batch_num * batch_size
          } else {
            batch_size
          };
          let replaced_sentence = limited_query_str
            .replace("$1", &batch_size.to_string())
            .replace("$2", &offset.to_string());

          if verbose {
            println!("{} secs \tthread {}: batch {}: executing sentence: {}",
              start.elapsed().as_secs(), thread_num, batch_num, replaced_sentence
            );
          }
          let query = &stmt.query(&[&batch_size, &offset]).unwrap();
          if verbose {
            println!("{} secs \tthread {}: batch {}: ... sentence executed. len is {}. writing to file",
              start.elapsed().as_secs(), thread_num, batch_num, query.len()
            );
          }
          for (i, row) in query.iter().enumerate()
          {
            if i == 0
            {
              if verbose {
                println!("{} secs \tthread {}: batch {}: starting writing to file {} rows",
                  start.elapsed().as_secs(), thread_num, batch_num, batch_size);
              }
            }

            for col in 0..row.len()
            {
              let val: String = row.get(col);
              file.write(val.as_bytes()).unwrap();
              file.write(separator.as_bytes()).unwrap();
            }
            file.write(b"\n").unwrap();
          }

          if verbose {
            println!("{} secs \tthread {}: batch {}: finished writing to file {} rows",
              start.elapsed().as_secs(), thread_num, batch_num, batch_size);
          }
        }
      }));
  }

  let mut i: i32 = 0;
  for child in children {
    // Wait for the thread to finish. Returns a result.
    if verbose {
      println!("{} secs \tbase_thread: finishing thread {}", start.elapsed().as_secs(), i);
    }

    let _ = child.join();

    if verbose {
      println!("{} secs \tbase_thread: ... finished thread {}", start.elapsed().as_secs(), i);
    }
    i = i + 1;
  }
}