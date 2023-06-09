from argparse import ArgumentParser
import os
import json
import gzip
import numpy as np
from tqdm import tqdm

def get_args():
    parser = ArgumentParser()
    parser.add_argument("--file-path", nargs='+', default=[], help="path to the files to read")
    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()
    lines_lines = []
    for fp in tqdm(args.file_path):
        if not os.path.exists(fp):
            raise ValueError(f"File does not Exists: {fp}")
        with gzip.open(fp, "rb") as f:
            for line in f:
                file_string = line.decode("utf-8")
        lines = []
        last = 0
        for i in range(1, len(file_string)):
            if file_string[i-1] == "}" and file_string[i] == "{":
                lines.append(file_string[last:i])
                last = i
        lines.append(file_string[last:])
        lines_lines.append(len(lines))
        #for line in lines:
            #print(json.loads(line))
        #print(f"There are {len(lines)} tweets in this file")
    all_tweets = np.array(lines_lines)
    print(f"Average per week: {np.mean(all_tweets)}")
    print(f"All tweets: {np.sum(all_tweets)}")
