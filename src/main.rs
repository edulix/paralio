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
use std::thread;

#[macro_use]
extern crate clap;

use clap::App;
use std::time::Instant;
use std::io::prelude::*;
use std::fs::File;
use std::path::Path;
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

  let njobs: i32 = matches.value_of("jobs").unwrap().parse().unwrap();
  let mut children = vec![];

  let start = Instant::now();
  println!("{} secs \tbase_thread: counting rows with sentence: {}", start.elapsed().as_secs(), count_query_str);
  let conn = Connection::connect(connection_str.as_str(), TlsMode::None).unwrap();
  let stmt = conn.prepare(&count_query_str).unwrap();
  let total_count: i64 = stmt.query(&[]).unwrap().get(0).get(0);
  let batch_size: i64 = (total_count + njobs as i64) / njobs as i64;
  println!("{} secs \tbase_thread: ... counted: {} total, {} batch_size for {} jobs", start.elapsed().as_secs(), total_count, batch_size, njobs);

  for thread_num in 0..njobs
  {
    let query_str = query_str.clone();
    let connection_str = connection_str.clone();
    let directory = directory.clone();
    let separator = separator.clone();
    let batch_size = batch_size.clone();

    children.push( thread::spawn(move || {
        let offset = thread_num as i64 * batch_size;
        let conn = Connection::connect(connection_str.as_str(), TlsMode::None).unwrap();
        let mut limited_query_str = String::new();
        write!(&mut limited_query_str, "{} limit {} offset {}", query_str, batch_size, offset).unwrap();

        println!("{} secs \tthread {}: executing sentence: {}", start.elapsed().as_secs(), thread_num, limited_query_str);
        let stmt = conn.prepare(&limited_query_str).unwrap();
        let path = Path::new(&directory).join(thread_num.to_string());
        let mut file = File::create(path).unwrap();
        let query = &stmt.query(&[]).unwrap();
        println!("{} secs \tthread {}: ... sentence executed. len is {}. writing to file", start.elapsed().as_secs(), thread_num, query.len());
        for (i, row) in query.iter().enumerate() {
          if i == 0 {
            println!("{} secs \tthread {}: starting writing to file", start.elapsed().as_secs(), thread_num);
          }
          for col in 0..row.len() {
            let val: String = row.get(col);
            file.write(val.as_bytes()).unwrap();
            file.write(separator.as_bytes()).unwrap();
          }
          file.write(b"\n").unwrap();
        }
        println!("{} secs \tthread {}: finished writing to file", start.elapsed().as_secs(), thread_num);
      }));
  }

  let mut i: i32 = 0;
  for child in children {
    // Wait for the thread to finish. Returns a result.
    println!("{} secs \tbase_thread: finishing thread {}", start.elapsed().as_secs(), i);
    let _ = child.join();
    println!("{} secs \tbase_thread: ... finished thread {}", start.elapsed().as_secs(), i);
    i = i + 1;
  }
}