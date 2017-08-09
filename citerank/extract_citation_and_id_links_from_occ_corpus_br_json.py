from __future__ import print_function
from __future__ import absolute_import

import argparse
import json
import csv
from glob import glob

from tqdm import tqdm

def parse_args():
  parser = argparse.ArgumentParser(
    description=
      'Extract br-citation links (CSV) and br-id links (CSV) from'
      ' OpenCitations Corpus (OCC) JSON files'
  )
  parser.add_argument(
    '--br-json-path', type=str, required=True,
    help='glob pattern to id json files'
  )
  parser.add_argument(
    '--br-citation-links-output-path', type=str, required=False,
    default='br-citation-links.csv',
    help='output filename to citation links (csv)'
  )
  parser.add_argument(
    '--br-id-links-output-path', type=str, required=False,
    default='br-id-links.csv',
    help='output filename to br to id links (csv)'
  )
  args = parser.parse_args()
  return args

def iter_extract_from_br_json_files(id_json_files):
  for filename in id_json_files:
    with open(filename, 'r') as json_f:
      id_content = json.load(json_f)
      graph_node = id_content['@graph']
      for n in graph_node:
        br_id = n['iri']
        title = n.get('title')
        identifiers = n.get('identifier', [])
        if not isinstance(identifiers, list):
          identifiers = [identifiers]
        citations = n.get('citation', [])
        if not isinstance(citations, list):
          citations = [citations]
        yield (br_id, title, identifiers, citations)

def extract_from_br_json_files(
  br_json_files,
  br_citation_links_output_path,
  br_id_links_output_path):

  with open(br_citation_links_output_path, 'w') as br_citation_links_f:
    with open(br_id_links_output_path, 'w') as br_gid_links_f:
      br_citation_links_writer = csv.writer(br_citation_links_f)
      br_citation_links_writer.writerow([
        'citing_br_id',
        'citing_title',
        'cited_br_id'
      ])

      br_id_links_writer = csv.writer(br_gid_links_f)
      br_id_links_writer.writerow(['br_id', 'id_id'])

      for (
        br_id,
        title,
        identifiers,
        citations
      ) in iter_extract_from_br_json_files(br_json_files):
        for cited_br_id in citations:
          br_citation_links_writer.writerow([
            br_id,
            (title or '').encode('utf-8'),
            cited_br_id
          ])
        for identifier_id in identifiers:
          br_id_links_writer.writerow([
            br_id,
            identifier_id
          ])

def main():
  args = parse_args()

  extract_from_br_json_files(
    tqdm(glob(args.br_json_path), leave=False),
    args.br_citation_links_output_path,
    args.br_id_links_output_path
  )

if __name__ == "__main__":
  main()
