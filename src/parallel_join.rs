/**
 * Copyright (C) 2017 Eduardo Robles Elvira <edulix@nvotes.com>

 * paralio is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License.

 * paralio is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.

 * You should have received a copy of the GNU Affero General Public License
 * along with paralio.  If not, see <http://www.gnu.org/licenses/>.
**/

use std::thread;
use std::time::Instant;
use std::path::Path;
use std::sync::mpsc::{Sender, Receiver};
use std::sync::mpsc;

use OutputFile;
use ByteRangeLineReader;

// Executes a skew partition parallel join algorithm
//
// 1. Divide the A file in N ranges (one per job/thread)
// 2. for each range:
// 2.1. find the range in B
// 2.2. spawn a thread with the batch
// 2.3. each thread merge joins
pub fn execute_parallel_join(
  file1_str_list: &Vec<String>,
  file2_str_list: &Vec<String>,
  separator: &String,
  field1: u32,
  field2: u32,
  output_fields_str_list: &Vec<String>,
  output_path: &String,
  verbose: bool,
  njobs: i32,
)
{
  let a_ranges = ByteRangeLineReader::open(&file1_str_list, njobs as u64, verbose);

  let mut children = vec![];

  let start = Instant::now();

  let mut ends: Vec<(Sender<u64>, Receiver<u64>)> = Vec::with_capacity(njobs as usize + 1);
  ends.push(mpsc::channel());
  ends[0].0.send(0).unwrap();

  for (thread_num, a_range) in a_ranges.iter().enumerate()
  {
    let verbose = verbose.clone();
    let separator = separator.clone();
    let output_path = output_path.clone();
    let output_fields_str_list = output_fields_str_list.clone();
    let field1 = field1.clone();
    let file2_str_list = file2_str_list.clone();
    let field2 = field2.clone();
    let start = start.clone();

    ends.push(mpsc::channel());

    let start_pos = ends[thread_num].1.recv().unwrap();
    let next_tx = ends[thread_num+1].0.clone();
    let a_range = a_range.clone();

    children.push( thread::spawn(move ||
    {
      let path = String::from(Path::new(&output_path).join(thread_num.to_string()).to_str().unwrap());
      if verbose {
        println!("thread {}: output path: {}", thread_num, path);
      }

      let mut out = OutputFile::new(
        separator,
        verbose,
        path,
        output_fields_str_list,
        field1,
        file2_str_list,
        field2,
        a_range,
        start_pos
      );
      next_tx.send(out.file2_end()).unwrap();
      out.file1_read_next();
      out.file2_read_next();

      if thread_num as i32 == njobs -1 && verbose {
        println!("thread={} elapsed={}s {}ns", thread_num, start.elapsed().as_secs(), start.elapsed().subsec_nanos());
      }

      while out.file1_has_current() && out.file2_has_current()
      {
        let key1 = out.file1_key();
        let key2 = out.file2_key();
        if verbose {
          println!("thread {} key1: {} key2: {}", thread_num, key1, key2);
        }

        match (Some(key1), Some(key2))
        {
          (Some(ref key1), Some(ref key2)) if key1 < key2 => {
            out.file1_read_next();
          },
          (Some(ref key1), Some(ref key2)) if key1 == key2 => {
            out.add_match();
            out.file1_read_next();
            out.file2_read_next();
          },
          (Some(ref key1), Some(ref key2)) if key1 > key2 => {
            out.file2_read_next();
          },
          _=> { break },
        }
      }
      if thread_num as i32 == njobs -1 && verbose {
        println!("thread={} END elapsed={}s {}ns", thread_num, start.elapsed().as_secs(), start.elapsed().subsec_nanos());
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

#[cfg(test)]
mod test
{
  use tempdir::TempDir;

  use test_helpers::_write_files;
  use test_helpers::_assert_files_eq;

  use execute_parallel_join;

  #[test]
  fn test_join1()
  {
    struct Data {
      file1_str: &'static str,
      file2_str: &'static str,
      separator: &'static str,
      field1: u32,
      field2: u32,
      output_fields_str_list: &'static str,
      output_str: &'static str,
      njobs: i32
    }

    let l: Vec<Data> = vec![
      Data {
        file1_str: "1,2,3,4",
        file2_str: "1,2,4,5,6",
        separator: ",",
        field1: 0,
        field2: 0,
        output_fields_str_list: "1.0,1.0",
        output_str: "1,1\n2,2\n4,4\n",
        njobs: 1
      },
      Data {
        file1_str: "1;aa,2;bb,3;cc,4;dd",
        file2_str: "1;AAAAAA,2;BBBBBBBB,4;CCCCC,5;DD,6;EEEEE",
        separator: ";",
        field1: 0,
        field2: 0,
        output_fields_str_list: "1.1,2.1",
        output_str: "aa;AAAAAA\nbb;BBBBBBBB\ndd;CCCCC\n",
        njobs: 1
      },
      Data {
        file1_str: "1;aa,2;bb,3;cc,4;dd",
        file2_str: "1;aa;AAAA,2;BBBBBBBB;42,4;cc;CCC,5;DD;ddd,6;EEEEE;",
        separator: ";",
        field1: 1,
        field2: 1,
        output_fields_str_list: "1.0,1.1,2.1,2.2",
        output_str: "1;aa;aa;AAAA\n3;cc;cc;CCC\n",
        njobs: 1
      },
      Data {
        file1_str: "1,2|3,4",
        file2_str: "1,2,4|5,6",
        separator: ",",
        field1: 0,
        field2: 0,
        output_fields_str_list: "1.0,1.0",
        output_str: "1,1\n2,2\n4,4\n",
        njobs: 1
      },
      Data {
        file1_str: "0,1,2,3,4,5,6,7,8,9",
        file2_str: "0,1,2,3,4,5,6,7,8,9",
        separator: ",",
        field1: 0,
        field2: 0,
        output_fields_str_list: "1.0",
        output_str: "0\n1\n2\n3\n4\n5\n|6\n7\n8\n9\n",
        njobs: 2
      },
      Data {
        file1_str: "0,1,2,3,4,5,6,7,8,9",
        file2_str: "0,1,2,3,4,5,6,7,9",
        separator: ",",
        field1: 0,
        field2: 0,
        output_fields_str_list: "1.0",
        output_str: "0\n1\n2\n3\n4\n5\n|6\n7\n9\n",
        njobs: 2
      },
      Data {
        file1_str: "0,1,2,3,4,5,6,7,8,9",
        file2_str: "5,6,7,8,9",
        separator: ",",
        field1: 0,
        field2: 0,
        output_fields_str_list: "1.0",
        output_str: "5\n|6\n7\n8\n9\n",
        njobs: 2
      },
      Data {
        file1_str: "0,1,2,3,4,5,6,7,8,9",
        file2_str: "6,7,8,9",
        separator: ",",
        field1: 0,
        field2: 0,
        output_fields_str_list: "1.0",
        output_str: "|6\n7\n8\n9\n",
        njobs: 2
      },
    ];
    for ref s in l.iter()
    {
      let tmp_dir_1 = TempDir::new("parallel_join_1").expect("create temp dir");
      let tmp_dir_2 = TempDir::new("parallel_join_2").expect("create temp dir");
      let tmp_dir_out = TempDir::new("parallel_join_out").expect("create temp dir");

      let files_1 = _write_files(s.file1_str, &tmp_dir_1);
      let files_2 = _write_files(s.file2_str, &tmp_dir_2);
      let tmp_dir_out_path = String::from(tmp_dir_out.path().to_str().unwrap());
      let separator = String::from(s.separator);
      let output_fields_str_list: Vec<String> = {
        s.output_fields_str_list.split(',').map(String::from).collect()
      };

      execute_parallel_join(
        &files_1,
        &files_2,
        &separator,
        s.field1,
        s.field2,
        &output_fields_str_list,
        &tmp_dir_out_path,
        false,
        s.njobs
      );
      _assert_files_eq(&tmp_dir_out_path, s.output_str);
    }
  }
}