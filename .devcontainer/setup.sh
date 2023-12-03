#!/bin/bash

# Install Astronomer CLI
curl -sSL install.astronomer.io | sudo bash -s

# Create the directory for models
mkdir -p ./models/bert-base-NER
mkdir -p ./models/bertweet-base-sentiment-analysis

# Clone the necessary repositories
git clone https://huggingface.co/dslim/bert-base-NER ./models/bert-base-NER
git clone https://huggingface.co/finiteautomata/bertweet-base-sentiment-analysis ./models/bertweet-base-sentiment-analysis

# Wait for Docker daemon to be ready
sleep 20

# Start the Astronomer development environment
astro dev start -n --wait 8m
