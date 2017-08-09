#!/bin/bash

set -e

TEMP_DIR=.temp

if [ -f .config ]; then
  source .config
fi

python -m citerank.extract_id_doi_map_from_occ_corpus_id_json\
  --id-json-path "$TEMP_DIR/corpus_id/**/*.json"\
  --doi-map-output-path "$TEMP_DIR/id-doi-map.csv" $@

python -m citerank.extract_citation_and_id_links_from_occ_corpus_br_json \
  --br-json-path "$TEMP_DIR/corpus_br/**/*.json"\
  --br-citation-links-output-path "$TEMP_DIR/br-citation-links.csv"\
  --br-id-links-output-path "$TEMP_DIR/br-id-links.csv"

python -m citerank.map_br_citation_links_to_doi\
  --br-citation-links-path "$TEMP_DIR/br-citation-links.csv"\
  --br-id-links-path "$TEMP_DIR/br-id-links.csv"\
  --id-doi-map-path "$TEMP_DIR/id-doi-map.csv"\
  --doi-citation-links-output-path "$TEMP_DIR/doi-citation-links.csv"
