from argparse import ArgumentParser
import os
import json
import gzip
import numpy as np
from tqdm import tqdm
import jsonlines

def get_args():
    parser = ArgumentParser()
    parser.add_argument("--output-path", help="path to the output dir")
    parser.add_argument("--file-path", nargs='+', default=[], help="path to the files to read")
    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()
    lines_lines = []
    for fp in tqdm(args.file_path):
        filename = os.path.basename(fp)
        if not os.path.exists(fp):
            raise ValueError(f"File does not Exists: {fp}")
        with gzip.open(fp, "rb") as f:
            for line in f:
                file_string = line.decode("utf-8")
        lines = []
        last = 0
        user_location = 0
        places = 0
        escaped = False
        for i in range(1, len(file_string)):
            if file_string[i] == '"' and file_string[i-1] != "\\":
                escaped = not escaped
            if file_string[i-1] == "}" and file_string[i] == "{":
                try:
                    tweet_obj = json.loads(file_string[last:i])
                except json.decoder.JSONDecodeError:
                    continue
                if 'location' in tweet_obj['author']:
                    user_location += 1
                tweet_obj['tweet']['user'] = tweet_obj['author']
                if len(list(tweet_obj.keys())) > 2:
                    places += 1
                    tweet_obj['tweet']['includes'] = {"places": [tweet_obj['place']]}
                lines.append(tweet_obj['tweet'])
                last = i
        try:
            tweet_obj = json.loads(file_string[last:])
        except json.decoder.JSONDecodeError:
            breakpoint()
        tweet_obj['tweet']['user'] = tweet_obj['author']
        if 'place' in tweet_obj:
            tweet_obj['tweet']['includes'] = {"places": [tweet_obj['place']]}
        lines.append(tweet_obj['tweet'])
        #print(f"user locations: {user_location}")
        #print(f"places: {places}")
        #print(f"tweets: {len(lines)}")
        with gzip.open(os.path.join(args.output_path, filename), "wb") as f:
            j_writer = jsonlines.Writer(f)
            j_writer.write_all(lines)
