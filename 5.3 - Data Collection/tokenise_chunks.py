import stanza
from pathlib import Path
import argparse
from typing import Iterable, TextIO
from stanza_batch import batch
import time
import os
import sys
import json
from datetime import datetime


def stanza_doc_to_dic(doc):
    words = []
    for sent in doc.sentences:
        for word in sent.words:
            word_dic = {"tok": word.text, 
                        "pos": word.upos, 
                        "feats": {feat.split("=")[0]: feat.split("=")[1] for feat in word.feats.split("|")} if word.feats is not None else None}
            words.append(word_dic)
            
    doc_dic = {"words": words, 
               "entities": [{"text": ent.text, "type": ent.type} for ent in doc.ents],
               "sentiment": sum([s.sentiment for s in doc.sentences]) / len(doc.sentences)}
    
    meta_dic = dict()
    meta_dic["num_words"] = len(doc_dic["words"])
    meta_dic["num_sentences"] = len(doc.sentences)
    meta_dic["num_entities"] = len(doc.ents)
    
    doc_dic["meta"] = meta_dic
    
    return doc_dic


def file_path(argument_str: str) -> Path:
    '''
    :param argument_str: String from a given argument.
    :returns: Converts argument String to Path type.
    '''
    return Path(argument_str).resolve()


def yield_posts(fp: Path) -> Iterable[str]:
    '''
    Given a file path to a json file containing a dict of uid: body pairs,
    yield each body individually.
    :param fp: File path to a text.
    :returns: Yields post bodies from the file in order from start of 
              file to the end.
    '''
    with fp.open('r', encoding='utf-8-sig') as _file:
        posts = json.load(_file)

    for body in posts.values():
        yield body


def yield_uids(fp: Path) -> Iterable[str]:
    '''
    Given a file path to a json file containing a dict of uid: body pairs,
    yield each uid individually.
    :param fp: File path to a text.
    :returns: Yields post uids from the file in order from start of 
              file to the end.
    '''
    with fp.open('r', encoding='utf-8-sig') as _file:
        posts = json.load(_file)

    for uid in posts:
        yield uid


if __name__ == '__main__':
    program_description = ('Process the text within the given file (1st argument) '
                           'using Stanza English NER model and writes all Entities to'
                           'a file with a json-formatted line for each contribution.')
    parser = argparse.ArgumentParser(description=program_description)
    parser.add_argument('text_file_path', type=file_path, 
                        help='File path to the text to process e.g. Alice in Wonderland.')
    parser.add_argument('output_file_path', type=file_path,
                        help='File path to output the processed data too.')
    parser.add_argument('batch_size', type=int, 
                        help='Number of paragraphs of text for SpaCy to process at a time.')
    parser.add_argument('stanza_model_directory', type=file_path, 
                        help='Directory to store the pre-trained stanza models.')
    args = parser.parse_args()

    text_fp = args.text_file_path
    output_fp = args.output_file_path
    batch_size = args.batch_size
    stanza_model_directory = args.stanza_model_directory
    stanza_model_directory.mkdir(exist_ok=True)

    # Download the relevant stanza model if it has not already been downloaded.
    stanza_processes = 'tokenize,pos,ner,sentiment'
    stanza.download("en", dir=str(stanza_model_directory), 
                    processors=stanza_processes)
    # load the stanza model
    nlp = stanza.Pipeline(lang='en', processors=stanza_processes, use_gpu=True,
                          tokenize_batch_size=batch_size,
                          ner_batch_size=batch_size,
                          dir=str(stanza_model_directory))
    
    # Load data
    posts_to_process = yield_posts(text_fp)
    uids_of_documents = yield_uids(text_fp)

    # Process data
    paragraph_number = 0
    processing_time: float = 0.0
    with output_fp.open('w', encoding="utf-8") as out_file:
        startTime = datetime.now()
        for uid, stanza_document in zip(uids_of_documents, batch(posts_to_process, nlp, batch_size=batch_size)):
            out_file.write(json.dumps([uid, stanza_doc_to_dic(stanza_document)]) + "\n")

        processing_time = datetime.now() - startTime
        print(f"Time Taken: {processing_time}")
