from __future__ import print_function
from __future__ import absolute_import

import argparse
import re
import json
import os
import errno

import requests
from lxml import html
from tqdm import tqdm

def parse_most_recent_download_links(html_content):
  page = html.fromstring(html_content)
  table = page.xpath("//table")[0]
  links_by_name = {}
  for tr in table.xpath(".//tr[td[2][//a]]"):
    name = tr.xpath('./td[1]/text()')[0].strip()
    links = {}
    for link in tr.xpath("./td[2]//a"):
      links[link.text.strip()] = link.get('href')
    links_by_name[name] = links
  return links_by_name

def resolve_figshare_url(figshare_url):
  figshare_id_match = re.match(r'.*figshare.*\D(\d+)$', figshare_url)
  if not figshare_id_match:
    raise Exception('unrecognised figshare url: ' + figshare_url)
  figshare_id = figshare_id_match.group(1)
  versions_url = "https://api.figshare.com/v2/articles/{}/versions".format(figshare_id)
  versions = sorted(
    json.loads(requests.get(versions_url).text),
    key=lambda v: v['version']
  )
  last_version_url = versions[-1]['url']
  version_details = json.loads(requests.get(last_version_url).text)
  version_files = version_details['files']
  version_biggest_file = sorted(
    version_files,
    key=lambda f: f['size']
  )[-1]
  return version_biggest_file['download_url']

def makedirs(path, exist_ok=False):
  try:
    # Python 3
    os.makedirs(path, exist_ok=exist_ok)
  except TypeError:
    # Python 2
    try:
      os.makedirs(path)
    except OSError as e:
      if e.errno != errno.EEXIST:
        raise

def download_to(url, target_filename):
  r = requests.get(url, stream=True)
  total_size = int(r.headers["Content-Length"])
  makedirs(os.path.dirname(target_filename), exist_ok=True)
  target_temp_filename = target_filename + '.part'
  if os.path.exists(target_filename):
    os.remove(target_filename)
  with open(target_temp_filename, "wb") as f:
    with tqdm(total=total_size, desc=target_filename, leave=False) as pbar:
      for chunk in r:
        f.write(chunk)
        pbar.update(len(chunk))
  os.rename(target_temp_filename, target_filename)

def parse_args():
  parser = argparse.ArgumentParser(
    description='Download OpenCitations Corpus (OCC)'
  )
  parser.add_argument(
    '--download-path', type=str, required=False,
    default='.temp',
    help='path to download directory')
  args = parser.parse_args()
  return args

def main():
  args = parse_args()

  download_url = "http://opencitations.net/download"
  html_content = requests.get(download_url).text
  links_by_name = parse_most_recent_download_links(html_content)
  id_data_url = links_by_name.get('identifiers (id)', {}).get('data', '')
  br_data_url = links_by_name.get('bibliographic resources (br)', {}).get('data', '')
  if id_data_url == '':
    raise Exception('identifiers data url not found (has the html changed?')
  if br_data_url == '':
    raise Exception('bibliographic resources data url not found (has the html changed?')
  id_data_download_url = resolve_figshare_url(id_data_url)
  br_data_download_url = resolve_figshare_url(br_data_url)
  download_to(
    id_data_download_url,
    os.path.join(args.download_path, 'corpus_id.zip')
  )
  download_to(
    br_data_download_url,
    os.path.join(args.download_path, 'corpus_br.zip')
  )

if __name__ == "__main__":
  main()
