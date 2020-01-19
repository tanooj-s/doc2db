import json
import time
from neo4j import GraphDatabase

print("Connecting to Neo4j database....")
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j","password"))


print("Reading json data....")
with open('data_extract.json','r') as f:
	data_objects = json.load(f)


def push_to_db(tx, data):
	# take in a data object from the dataframe and write parameterized Cypher queries to push to DB
	tx.run("CREATE (a:Company {name: $name}) "
		"MERGE (a)-[:HAD_MEETING]->(b:Board_meeting {date: $meeting_date, purpose: $purpose, link: $link})",
	 name=data['company'],
	 meeting_date=data['board_meeting']['date'],
	 purpose=data['board_meeting']['purpose'],
	 link=data['board_meeting']['link'] )
	# if data has keys for financial report, personnel, funds, then add nodes and relationships to meeting
	print("pushed meeting of " + str(data['company']) + " to neo4j db")

	if "financial_results" in data.keys():
		tx.run("MATCH (b:Board_meeting {link: $link}) "
			"MERGE (f:Financial_results {period: $period, type: $type}) "
			"MERGE (b)-[:REPORTED]->(f)",
			link=data['board_meeting']['link'],
			period=data['financial_results']['period'],
			type=data['financial_results']['type'])
		print("pushed financial results of " + str(data['company']) + " to neo4j db")

	if "personnel" in data.keys():
		tx.run("MATCH (b:Board_meeting {link: $link}) "
			"MERGE (p:Personnel {resignation: $resignation, appointment: $appointment}) "
			"MERGE (b)-[:REAPPOINTMENT]->(p)",
			link=data['board_meeting']['link'],
			resignation=data['personnel']['resignation'],
			appointment=data['personnel']['appointment'])
		print("pushed personnel changes of " + str(data['company']) + " to neo4j db")

	if "funds" in data.keys():
		tx.run("MATCH (b:Board_meeting {link: $link}) "
			"MERGE (fn:Funds {type: $type}) "
			"MERGE (b)-[:PROPOSED_FUNDS]->(fn)",
			link=data['board_meeting']['link'],
			type=data['funds'])
		print("pushed propsosed funds of " + str(data['company']) + " to neo4j db")

	if "merge_companies" in data.keys():
		tx.run("MATCH (b:Board_meeting {link: $link}) "
			"MERGE (m:Merge_companies {entities: $entities}) "
			"MERGE (b)-[:PROPOSED_MERGER]->(m)",
			link=data['board_meeting']['link'],
			entities=data['merge_companies'])
		print("pushed potential merging companies of " + str(data['company']) + " to neo4j db")

	return True


print("Starting to push data to Neo4j......")
start = time.time()
with driver.session() as session:
	for data_obj in data_objects:
		session.write_transaction(push_to_db, data_obj)
		print("pushed data for " + data_obj['company'])
end = time.time()
print ("Took " + str(end-start) + " seconds to push data from " + str(len(data_objects)) + " documents to database")
