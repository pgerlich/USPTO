import sys
import fileinput
import redis
import time
import datetime
from lxml import etree

def main():
	try:
		inputXMLFilePath = sys.argv[1] #First input is XML file
	except IndexError:
		print "Incorret syntax. Expecting python parseXML.py input.xml"
		return

	#Connect to redis server
	rServer = redis.StrictRedis(host='localhost', port=6379, db=0)

	#Files have to be pre-processed to match a desired XML tree format for iteration
	if not ".pp" in inputXMLFilePath:
		print "Preprocessing", inputXMLFilePath
		preprocess_xml_file(inputXMLFilePath)
		inputXMLFilePath =  inputXMLFilePath + ".pp"
		print "Preprocessing complete"

	#Open our input file for processing
	inputFile = open(inputXMLFilePath,'r');

	#Need tree for xpath, root for iteration
	tree = etree.parse(inputFile)
	root = tree.getroot()

	index = 0

	#Each child is a patent
	for child in root:
		print child.tag
		convert_xml_patent_to_rlist(rServer, tree, child, False, True, index)
		index += 1
		if ( index > 1 ): #TODO: Remove this. Just in here to prevent huge data dumps and demo functionality
			break


#Parses USPTO XML document and creates a dictionary of attributes/values
#Will pretty print if prettyPrint = true
#Will create list if createList = true
def convert_xml_patent_to_rlist(rServer, tree, root, prettyPrint, createList, index):
	patentDictionary = {};

	for element in root.iter():
		#If we have attributes of an element, we want to store those as K:V pairs too
		if len(element.attrib):
			for attribute in element.attrib:
				curAttribPath = tree.getpath(element) + '/' + attribute
				curAttribValue = element.attrib[attribute].encode("utf-8")
				if prettyPrint:
					print "(Attribute)" , curAttribPath , ':' , curAttribValue
				if createList:
					patentDictionary[curAttribPath] = curAttribValue

		#not attributes of element, but text inside element
		if element.text and (len(element.text) > 1):
			curElementPath = convertKey(tree.getpath(element)) #Convert these keys if applicable to standardize information for queries
			curElementValue = element.text.encode("utf-8")

			if 'Date' in curElementPath:
				curElementValue = convertDateToTimestamp(curElementValue)
				
			if prettyPrint:
				print "(Value)" , curElementPath , ':' , curElementValue

			if createList:
				patentDictionary[curElementPath] = curElementValue

	if createList:
		#Key is just the time since epoch + index - guarenteed unique unless run in parallel or something weird
		patentKey = str(int(time.time()) + int(index)) #Index (per file) + timestamp = Unique Identifier

		rServer.hmset(patentKey, patentDictionary)

		indexDictionary(rServer, patentDictionary, patentKey) #Indexying for querying

		print "Stored patent with key", patentKey

#Given an input XML file, remove the doctype and xml version declarations and replace them with a single wrapper, for parsing by XMLTree
def preprocess_xml_file(inputFilePath):
	inputFile = open(inputFilePath,'r');
	outputFile = open(inputFilePath + ".pp", 'w')

	#Create a wrapper XML element for iterating over the patent elements
	outputFile.write("<wrapper>")
	
	for line in inputFile:
		if not "DOCTYPE" in line and not "xml version" in line:
			outputFile.write(line)

	outputFile.write("</wrapper>")

def convertKey(key):
	if key == "/wrapper/us-patent-grant/us-bibliographic-data-grant/publication-reference/document-id/date":
		return "IssueDate"
	if key == "/wrapper/us-patent-grant/us-bibliographic-data-grant/application-reference/document-id/date":
		return "ApprovalDate"
	if key == "/wrapper/us-patent-grant/us-bibliographic-data-grant/invention-title":
		return "Title"
	if key == "/wrapper/us-patent-grant/claims/claim/claim-text":
		return "Description"

	return key

def convertDateToTimestamp(date):
	year = date[:4].strip()
	month = date[4:6].strip()
	day = date[6:8].strip()

	timeString = day + "/" + month + "/" + year

	total = time.mktime(datetime.datetime.strptime(timeString, "%d/%m/%Y").timetuple())

	#Some weirdness with these convertions, so we subtract the "epoch"

	#print "Year:", year, "Month:", month, "Day:", day, "Timestamp:", total
	return total

def indexDictionary(rServer, dictionary, key):
	#Making ApprovalDate queryable
	if "ApprovalDate" in dictionary:
		print "Indexed", key, "by approval date", dictionary['ApprovalDate']
		rServer.zadd("ApprovalDate", dictionary["ApprovalDate"], key) #Adds this lists KEY to an ordered set for querying. Rank is the date

	#Making Issued Date queryable
	if "IssueDate" in dictionary:
		print "Indexed", key, "by issue date", dictionary['IssueDate']
		rServer.zadd("IssueDate", dictionary["IssueDate"], key) #Adds this lists KEY to an ordered set for querying. Rank is the date

	if "Title" in dictionary:
		print "Indexed", key, "by Title", dictionary['Title']
		rServer.sadd('Title', dictionary['Title'] + ":" + key)

	if "Description" in dictionary:
		print "Indexed", key, "by Description", dictionary['Description']
		rServer.sadd('Description', dictionary['Description'] + ":" + key)
		
if __name__ == '__main__':
    main()

