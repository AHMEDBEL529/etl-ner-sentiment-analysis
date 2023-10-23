#!/bin/bash

# Create the directory for models
mkdir -p /workspaces/etl-ner-sentiment-analysis/models/bert-base-NER
mkdir -p /workspaces/etl-ner-sentiment-analysis/models/bertweet-base-sentiment-analysis

# Clone the necessary repositories
git clone https://huggingface.co/dslim/bert-base-NER /workspaces/etl-ner-sentiment-analysis/models/bert-base-NER
git clone https://huggingface.co/finiteautomata/bertweet-base-sentiment-analysis /workspaces/etl-ner-sentiment-analysis/models/bertweet-base-sentiment-analysis

# Start the Astronomer development environment
astro dev start -n --wait 8m
