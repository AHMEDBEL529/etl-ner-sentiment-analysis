#!/bin/bash

# Install Astronomer CLI
curl -sSL install.astronomer.io | sudo bash -s

# Create the directory for models
mkdir -p ./models/span-marker-mbert-base-multinerd
mkdir -p ./models/bart-large-mnli

# Clone the necessary repositories
git clone https://huggingface.co/tomaarsen/span-marker-mbert-base-multinerd ./models/span-marker-mbert-base-multinerd
git clone https://huggingface.co/facebook/bart-large-mnli ./models/bart-large-mnli

# Wait for Docker daemon to be ready
sleep 20

# Start the Astronomer development environment
astro dev start -n --wait 8m
