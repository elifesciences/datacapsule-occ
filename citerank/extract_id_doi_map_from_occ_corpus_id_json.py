from __future__ import print_function
from __future__ import absolute_import

import argparse
import json
import csv
from glob import glob

from tqdm import tqdm

def parse_args():
  parser = argparse.ArgumentParser(
    description='Extract DOI map (CSV) from OpenCitations Corpus (OCC) JSON files'
  )
  parser.add_argument(
    '--id-json-path', type=str, required=True,
    help='glob pattern to id json files'
  )
  parser.add_argument(
    '--doi-map-output-path', type=str, required=False,
    default='doi-map.csv',
    help='output filename to doi map (csv)'
  )
  args = parser.parse_args()
  return args

def iter_extract_from_id_json_files(id_json_files):
  for filename in id_json_files:
    with open(filename, 'r') as json_f:
      id_content = json.load(json_f)
      graph_node = id_content['@graph']
      for n in graph_node:
        if n.get('type') == 'doi':
          yield (n['iri'], n['id'])

def extract_from_id_json_files(id_json_files, doi_map_output_path):
  with open(doi_map_output_path, 'wb') as doi_map_f:
    doi_map_writer = csv.writer(doi_map_f)
    doi_map_writer.writerow(['id', 'doi'])
    for iri, doi in iter_extract_from_id_json_files(id_json_files):
      doi_map_writer.writerow([iri, doi.encode("utf-8")])

def main():
  args = parse_args()

  extract_from_id_json_files(
    tqdm(glob(args.id_json_path), leave=False),
    args.doi_map_output_path
  )

if __name__ == "__main__":
  main()
