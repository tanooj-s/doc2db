print ("Importing modules...")
import time
import re
import tqdm
import argparse
import numpy as np
import pandas as pd

from transformers import pipeline
import spacy

from neo4j import GraphDatabase

parser = argparse.ArgumentParser()
parser.add_argument('-input_file', action='store', dest='input_file')
arguments = parser.parse_args()
tqdm.pandas(desc='Progress bar')

print("Loading language model....")
nlp = spacy.load('en_core_web_md')

print("Loading question answering model from Transfomers...")
qa_model = pipeline(task = "question-answering", model = 'distilbert-base-uncased-distilled-squad', tokenizer = 'distilbert-base-uncased') 
# try other slower models that may be more accurate, although distilbert is 6x faster than BERT

def call_model(question, context):
# helper func to pull out answer from pipeline, but can use score, start/end later on for fine tuning model
	return qa_model({
		'question': question,
		'context': context
		})['amswer']


print("Reading input file....")
df = pd.read_excel(arguments.input_file) # or read csv, depending on input

def make_full_context(company,link,text):
# totally dependent on input data format, will need to generalize
	return {
	'company': company,
	'link': link,
	'text': text
	}
df['full_context'] = df.apply(lambda row: make_full_context(row['company'],row['link'],row['context']), axis=1)


def get_merger(context):
# function to get companies being merged, might be able to use similar logic for other documents as well
  s = re.findall(r'(merge|acqui|malgama)', context)
  if len(s) < 1:
    return False
  else:
    period_indices = [index for index, char in enumerate(context) if char == '.']
    merger_index = context.index(s[0])
    if len(period_indices) > 0:
      sentence_end = [i for i in period_indices if i > merger_index][0]
      if merger_index < period_indices[0]: # if merger/amalgamation is mentioned in the first sentence
        sentence = context[merger_index:period_indices[0]]
      else:
        sentence_start = [i for i in period_indices if i < merger_index][-1]
        sentence = context[sentence_start:sentence_end]
    else:
      sentence = context # if no periods just get orgs from the whole context (not a great hack)
  return [ent.text for ent in nlp(sentence).ents if ent.label_=="ORG"]


def get_document_data(context):
  # create nested dict (json) of unstructured data in document
  # context is a dict of the form { 'company': ___________, 'link': ____________, 'text': __________ }
  # ideally there will also be an ocr processing step so that a link to the raw document pdf/jpeg is sent in
  start = time.time()
  text = context['text']

  data = dict()
  data['company'] = context['company']
  data['board_meeting'] = dict()
 

  data['board_meeting']['date'] = call_model("when is the board meeting?", text)
  data['board_meeting']['purpose'] = call_model("what is being considered?", text)
  data['board_meeting']['link'] = context['link']

  #data['trading_window'] = call_model("when is the trading window closed?", text) need to improve logic for this
  if ('resign' in text.lower()) or ('appoint' in text.lower()):
    data['personnel'] = dict()
    data['personnel']['resignation'] = call_model("who is resigning?", text)
    data['personnel']['appointment'] = call_model("who is being appointed?", text)

  if "financial" in text.lower():
    data['financial_results'] = dict()
    data['financial_results']['period'] = call_model("when are financial results for?", text)
    data['financial_results']['type'] = call_model("are financial results audited or unaudited?", text)

  if len(re.findall(r'(merge|acqui|malgama)',text)) > 0: # use regex to find presence of acquisition/merger/amalgamation
    data['merge_companies'] = get_merger(text)

  if ("issue" in text.lower()) or ("issuance" in text.lower()):
    data['funds'] = call_model("what is being issued?", text) # add better logic for different kinds of funds later on, potentially could be better as a keyword search like we're doing for merger
  
  end = time.time()
  print("queried data from document, took " + str(end-start) + " seconds")

  return data


# could be pushing these directly to the database instead
print("Starting to analyse documents...........")
start = time.time()
df['data'] = df['full_context'].progress_apply(get_document_data)
end = time.time()
print("")
print ("Took %s seconds to get data from all documents"%(str(end-start)))

print("Collecting data into a JSON...")
with open('data_extract.json', 'w') as f:
	json.dump(list(df['data'].values), f)
print("Data ready to push to a Neo4j instance as a JSON file.")



#{ company: ______, board_meeting: { date: ________, purpose: _______, link:________, }, financial_results: { type: audited/unaudited, period: ___________ }, personnel: { resignation: ________, appointment: __________ }, merge_companies: [_______, __________], funds: ____________ }
 
