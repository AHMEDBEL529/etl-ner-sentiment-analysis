#!/bin/bash

# Create the directory for models
mkdir -p /workspaces/etl-ner-sentiment-analysis/models/

# Clone the necessary repositories
git clone https://huggingface.co/dslim/bert-base-NER /workspaces/etl-ner-sentiment-analysis/models/
git clone https://huggingface.co/finiteautomata/bertweet-base-sentiment-analysis /workspaces/etl-ner-sentiment-analysis/models/

# Start the Astronomer development environment
astro dev start -n --wait 8m
