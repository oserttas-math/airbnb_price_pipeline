#!/usr/bin/env python
"""
 An example of a step using MLflow and Weights & Biases to clean data
"""
import argparse
import logging
from sys import last_value
import wandb
import os
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()

    ######################
    # YOUR CODE HERE     #
    ######################
    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)
    logger.info("Downloading artifact")
    artifact_file = run.use_artifact(args.input_artifact)
    artifact_local_path = artifact_file.file()

    # read the csv file into a dataframe
    logger.info("Creating Raw DataFrame from Artifact")
    data_frame_raw = pd.read_csv(artifact_local_path)

    
    MIN_PRICE = args.min_price
    MAX_PRICE = args.max_price
    logger.info("Dropping Extreme Price Valeus Outside User Defined Range: %s-%s",
                MIN_PRICE, MAX_PRICE
                )
    idx = data_frame_raw['price'].between(MIN_PRICE,
        MAX_PRICE)
    data_frame_processed = data_frame_raw[idx].copy()
    # Fixing the error caught when running Sample2.csv file 
    # we need to make sure long is between -74.24 and -73.30 
    # also lat is between 40.5 and 41.2
    long_values = (-74.25, -73.50)
    lat_values = (40.5, 41.2)
    logger.info("Filtering Locational Boundaries of Lat: %s, long: %s", long_values, lat_values
    )
    idx_two = (
        data_frame_processed["longitude"].between(long_values) & 
        data_frame_processed["latitude"].between(lat_values)
    )

    data_frame_processed = data_frame_processed[idx_two].copy()
    
    logger.info("Casting last_review Field to Datetime")
    # convert string type to datetime
    data_frame_processed['last_review'] = pd.to_datetime(
        data_frame_processed['last_review'],
        errors='coerce'
        )
    
    logger.info("Saving Cleaned Data")
    file_to_save = "clean_sample.csv"
    data_frame_processed.to_csv(file_to_save, index=False)

    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description
    )

    logger.info("Uploading Cleaned Data to Wandb")
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)

    artifact.wait()

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description= "Simple Data Cleaning Step")

    parser.add_argument(
        "--input_artifact",
        type=str,
        help="Input artifact name",
        required=True
    )

    parser.add_argument(
        "--output_artifact",
        type=str,
        help="Output artifact name",
        required=True
    )

    parser.add_argument(
        "--output_type",
        type=str,
        help="Output artifact type",
        required=True
    )

    parser.add_argument(
        "--output_description",
        type=str,
        help="Output artifact description",
        required=True
    )

    parser.add_argument(
        "--min_price",
        type=float,
        help="Minimum rental price limit",
        required=True
    )

    parser.add_argument(
        "--max_price",
        type=float,
        help="Maximum rental price limit",
        required=True
    )

    arg_vars = parser.parse_args()

    go(arg_vars)












