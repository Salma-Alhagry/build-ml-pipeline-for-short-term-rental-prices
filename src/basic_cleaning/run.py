#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb

import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)
    
    logger.info(f"get the input artifact: {args.input_artifact}")
    artifact_local_path = run.use_artifact(args.input_artifact).file()
    
    df = pd.read_csv(artifact_local_path)

    df = df[df['price'].between(args.min_price, args.max_price)]
    logger.info(f"Cleaning step1: Drop the price outlier and keep the price between {args.min_price}$ and {args.max_price}$")
    
    df['last_review'] = pd.to_datetime(df['last_review'])
    logger.info(f"fix the type of last_review")
    
    df = df[df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)]
    logger.info(f"limit the area to nyc area")
    
    df.to_csv("clean_sample.csv", index=False)
    logger.info(f"data cleaned and saved to csv file")
    
    logger.info(f"uplading data to W&B")
    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)
    
    artifact.wait()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="The input artifact",
        required=True
    )

    parser.add_argument(
        "-- output_artifact", 
        type=str,
        help="The name of output artifact",
        required=True
    )

    parser.add_argument(
        "-- output_type", 
        type=str,
        help="The type of the output artifact",
        required=True
    )

    parser.add_argument(
        "-- output_description", 
        type=str,
        help="The description of the output artifact",
        required=True
    )

    parser.add_argument(
        "-- min_price", 
        type=float,
        help="The minimum price to consider",
        required=True
    )

    parser.add_argument(
        "-- max_price", 
        type=float,
        help="The Maximum price to consider",
        required=True
    )


    args = parser.parse_args()

    go(args)
