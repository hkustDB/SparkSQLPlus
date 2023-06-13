#!/bin/bash

SCRIPT=$(readlink -f $0)
SCRIPT_PATH=$(dirname "${SCRIPT}")

cd "${SCRIPT_PATH}"

# download bitcoin from https://snap.stanford.edu/data/soc-sign-bitcoin-alpha.html
if [[ -f graph.dat ]]; then
  echo "graph.dat already exists!"
  exit 1
else
  rm -f soc-sign-bitcoinalpha.csv
  rm -f soc-sign-bitcoinalpha.csv.gz

  curl -O https://snap.stanford.edu/data/soc-sign-bitcoinalpha.csv.gz > /dev/null 2>&1
  gzip -d soc-sign-bitcoinalpha.csv.gz
  mv soc-sign-bitcoinalpha.csv graph.dat

  rm -f soc-sign-bitcoinalpha.csv
  rm -f soc-sign-bitcoinalpha.csv.gz
fi