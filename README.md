Overview
--------

Downloads and processes citation data from [OpenCitations](http://opencitations.net/).

Pre-requisites
--------------
* Python 2 or 3
* [DAR](http://dar.linux.free.fr/)

Configure
---------

Optionally create `.config` file with the following content:

```bash
TEMP_DIR=<some other temp directory>
```

By default it will use `.temp`.

Download and Extract
--------------------

To download and extract:

`./download_and_extract_from_occ_corpus.sh`

Alternatively use one of the other bash scripts to run an individual step.

The final output will be `doi-citation-links.csv` with the columns: `citing_doi`, `citing_title`, `cited_doi` (~1 GB, 100 MB compressed)

