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

#[macro_use]
extern crate clap;

extern crate paralio;

use clap::App;

use paralio::execute_parallel_join;

fn main()
{
  // Executes a parallel join
  //
  // 1. Divide the A file in N ranges (one per job/thread)
  //
  // 2. for each range:
  // 2.1. find the range in B
  // 2.2. spawn a thread with the batch
  // 2.3. each thread merge joins

  let yaml = load_yaml!("pjoin.yml");
  let matches = App::from_yaml(yaml).get_matches();

  let file1_str_list: Vec<String> = matches.values_of("file1").unwrap()
    .map(String::from).collect();

  let file2_str_list: Vec<String> = matches.values_of("file2").unwrap()
    .map(String::from).collect();

  let separator = matches.value_of("separator").unwrap().to_string();

  let field1: u32 = matches.value_of("field1").unwrap().parse().unwrap();

  let field2: u32 = matches.value_of("field2").unwrap().parse().unwrap();

  let buffer_size: u32 = matches.value_of("field2").unwrap().parse().unwrap();

  let output_fields_str_list: Vec<String> = matches.values_of("output-fields")
    .unwrap().map(String::from).collect();

  let output_path = matches.value_of("output").unwrap().to_string();

  let verbose: bool = matches.is_present("verbose");

  let njobs: i32 = matches.value_of("jobs").unwrap().parse().unwrap();

  execute_parallel_join(
    &file1_str_list,
    &file2_str_list,
    &separator,
    field1,
    field2,
    &output_fields_str_list,
    &output_path,
    verbose,
    njobs,
    buffer_size
  );

}
