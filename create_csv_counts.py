from argparse import ArgumentParser
import os
import json
import gzip
import numpy as np
from tqdm import tqdm
import jsonlines
from dateutil import parser as time_parser
import pandas as pd
import spacy
from spacy.matcher import Matcher

def get_args():
    parser = ArgumentParser()
    parser.add_argument("--output-path", help="path to the output file")
    parser.add_argument("--keys", help="keywords to search over the tweets")
    parser.add_argument("--input-files", nargs='+', default=[], help="path to the files to read")
    return parser.parse_args()

def get_keywords(keyword):
    with open("/export/c11/caguirr/tweet-collection/keywords.json") as f:
        keyword_dict = json.load(f)
        if keyword in keyword_dict:
            return keyword_dict[keyword]
        else:
            raise ValueError("argument keys is not in keyword list")


if __name__ == "__main__":
    # SPACY SETUP
    nlp = spacy.load("en_core_web_sm")
    matcher = Matcher(nlp.vocab)
    # states
    with open("states.json") as f:
        states = json.load(f)
        STATES = {}
        for s in states:
            STATES[s['states']] = s['abbr']
    # SET KEYWORDS
    args = get_args()
    keywords = get_keywords(args.keys)
    for keyword in keywords:
        pattern = [{"LOWER": keyword}]
        matcher.add(keyword, [pattern])
    
    data = []
    for fp in tqdm(args.input_files):
        filename = os.path.basename(fp)
        if not os.path.exists(fp):
            raise ValueError(f"File does not Exists: {fp}")
        with gzip.open(fp, "rb") as f:
            jreader = jsonlines.Reader(f)
            file_dict = {k:0 for k in keywords}
            file_dict['Dates'] = time_parser.parse(filename.split("_")[0])
            for obj in jreader:
                tweet = json.loads(obj)
                #breakpoint()
                # keyword matching
                doc = nlp(tweet['text'])
                matches = matcher(doc)
                for match in set([nlp.vocab[m[0]].text for m in matches]):
                    file_dict[match] += 1
                # location
                if 'location' not in tweet:
                    continue
                if 'country' in tweet['location']:
                    if tweet['location']['country'] in file_dict:
                        file_dict[tweet['location']['country']] += 1
                    else:
                        file_dict[tweet['location']['country']] = 1
                if 'state' in tweet['location'] and tweet['location']['country'] == 'United States':
                    if tweet['location']['state'] in STATES:
                        state = STATES[tweet['location']['state']]
                        if state in file_dict:
                            file_dict[state] += 1
                        else:
                            file_dict[state] = 1
                if 'city' in tweet['location'] and tweet['location']['country'] == 'United States':
                    state = STATES[tweet['location']['state']]
                    city = f"{state}-{tweet['location']['city']}"
                    if city in file_dict:
                        file_dict[city] += 1
                    else:
                        file_dict[city] = 1
            data.append(file_dict)
    print("SAVING DATA")
    df = pd.DataFrame(data)
    df.to_csv(args.output_path, index=False)
