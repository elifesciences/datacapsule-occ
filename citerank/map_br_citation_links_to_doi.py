from __future__ import print_function
from __future__ import absolute_import

import argparse
import csv

def parse_args():
  parser = argparse.ArgumentParser(
    description='Map citation links\' ids to dois (CSV)'
  )
  parser.add_argument(
    '--br-citation-links-path', type=str, required=False,
    default='br-citation-links.csv',
    help='path to br citation links (csv)'
  )
  parser.add_argument(
    '--br-id-links-path', type=str, required=False,
    default='br-id-links.csv',
    help='path to br to id links (csv)'
  )
  parser.add_argument(
    '--id-doi-map-path', type=str, required=False,
    default='id-doi-map.csv',
    help='path to id to doi map (csv)'
  )
  parser.add_argument(
    '--doi-citation-links-output-path', type=str, required=False,
    default='doi-citation-links.csv',
    help='output filename to doi citation links (csv)'
  )
  args = parser.parse_args()
  return args

def iter_read_csv_columns(filename, columns):
  with open(filename, 'r') as csv_f:
    csv_reader = csv.reader(csv_f)
    first_row = True
    indices = None
    for row in csv_reader:
      if first_row:
        indices = [row.index(c) for c in columns]
        first_row = False
      else:
        yield [row[i] for i in indices]

def read_one_to_one_map(filename, key_column_name, value_column_name):
  columns = [key_column_name, value_column_name]
  return {
    key: value
    for key, value in iter_read_csv_columns(filename, columns)
  }

def groupby_key_to_dict(key_values):
  m = {}
  for key, value in key_values:
    values = m.get(key)
    if values:
      values.append(value)
    else:
      m[key] = [value]
  return m

def read_one_to_many_map(filename, key_column_name, value_column_name):
  columns = [key_column_name, value_column_name]
  return groupby_key_to_dict(
    iter_read_csv_columns(filename, columns)
  )

def iter_map_value_to_doi(key_values, id_doi_map):
  for key, value in key_values:
    doi = id_doi_map.get(value, '')
    if doi:
      yield key, doi

def read_id_doi_map(filename):
  return read_one_to_one_map(filename, 'id', 'doi')

def read_br_id_as_br_doi_map(filename, id_doi_map):
  columns = ['br_id', 'id_id']
  return {
    key: value
    for key, value in iter_map_value_to_doi(
      iter_read_csv_columns(filename, columns),
      id_doi_map
    )
  }

def iter_read_br_citations_as_doi_citations(filename, br_doi_map):
  columns = ['citing_br_id', 'citing_title', 'cited_br_id']
  for citing_br_id, citing_title, cited_br_id in iter_read_csv_columns(filename, columns):
    citing_doi = br_doi_map.get(citing_br_id, '')
    cited_doi = br_doi_map.get(cited_br_id, '')
    if citing_doi and cited_doi:
      yield citing_doi, citing_title, cited_doi

def main():
  args = parse_args()

  print('reading', args.id_doi_map_path)
  id_doi_map = read_id_doi_map(args.id_doi_map_path)
  print(len(id_doi_map))

  print('reading', args.br_id_links_path)
  br_doi_map = read_br_id_as_br_doi_map(args.br_id_links_path, id_doi_map)
  print(len(br_doi_map))

  print('writing to', args.doi_citation_links_output_path)
  with open(args.doi_citation_links_output_path, 'w') as doi_citation_links_f:
    doi_citation_links_writer = csv.writer(doi_citation_links_f)
    doi_citation_links_writer.writerow([
      'citing_doi',
      'citing_title',
      'cited_doi'
    ])
    for (
      citing_doi,
      citing_title,
      cited_doi
    ) in iter_read_br_citations_as_doi_citations(args.br_citation_links_path, br_doi_map):
      doi_citation_links_writer.writerow([citing_doi, citing_title, cited_doi])

if __name__ == "__main__":
  main()
