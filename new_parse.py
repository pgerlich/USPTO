import sys
import os
import fileinput
import re
import redis
import os.path
import time
import datetime
from lxml import etree

def main():
        try:
                inputFilePath = sys.argv[1] #First input is XML file
        except IndexError:
                print "Incorret syntax. Expecting python parse.py inputFileOrDirectory"
                return

        if os.path.isdir(inputFilePath):
                fileDir = os.listdir(inputFilePath)

                fileMap = {}

                index = 0

                for f in fileDir:
                        newInputFilePath = inputFilePath + "/" + f

                        #Connect to redis server
                        rServer = redis.StrictRedis(host='localhost', port=6379, db=0) #NOTE: Using DB 0

                        if '.txt' == f[-4:]:
                                print 'parsing text file: ', newInputFilePath
                                inputFile = open(newInputFilePath,'r');
                                index = index + convert_text_patent_to_rlist(rServer, inputFile, inputFilePath, index)

                                continue
                        elif '.xml'  == f[-4:]:
                                print 'parsing xml file: ', newInputFilePath

                                #Files have to be pre-processed to match a desired XML tree format for iteration
                        if not ".pp" in newInputFilePath:
                                # Preproess if necessary
                                if not os.path.isfile(newInputFilePath + ".pp"):
                                        print "Preprocessing", newInputFilePath
                                        preprocess_xml_file(newInputFilePath)
                                        print "Preprocessing complete"

                                newInputFilePath =  newInputFilePath + ".pp"

                        if newInputFilePath in fileMap:
                                continue

                        fileMap[newInputFilePath] = True

                        #Open our input file for processing
                        inputFile = open(newInputFilePath,'r');

                        #Need tree for xpath, root for iteration
                        tree = etree.parse(inputFile)
                        root = tree.getroot()

                                #Each child is a patent
                        for child in root:
                                convert_xml_patent_to_rlist(rServer, tree, child, index, newInputFilePath)
                                index = index + 1

                        index = 0
                        print "Parsing complete."


#Parses USPTO XML document and creates a dictionary of attributes/values
def convert_xml_patent_to_rlist(rServer, tree, root, index, fileName):
    patentDictionary = {};

    for element in root.iter():
            #If we have attributes of an element, we want to store those as K:V pairs too
            if len(element.attrib):
                for attribute in element.attrib:
                    curAttribPath = tree.getpath(element) + '/' + attribute
                    curAttribValue = element.attrib[attribute].encode("utf-8")

                    patentDictionary[curAttribPath] = curAttribValue

            #not attributes of element, but text inside element
            if element.text and (len(element.text) > 1):
                curElementPath = convertXMLKey(tree.getpath(element)) #Convert these keys if applicable to standardize information for queries
                curElementValue = element.text.encode("utf-8")

                if 'Date' in curElementPath:
                    curElementValue = convertDateToTimestamp(curElementValue)

                patentDictionary[curElementPath] = curElementValue

    patentKey = str(fileName) + '~' + str(index) #global file indexing

    rServer.hmset(patentKey, patentDictionary)

    indexDictionary(rServer, patentDictionary, patentKey) #Indexying for querying

    if int(patentKey.split('~')[1]) % 250 == 0:
        print "Stored patent with key", patentKey


#Create a redis list if createList is true
def convert_text_patent_to_rlist(rServer, inputFile, fileName, index):
        patentDictionary = {}

        curKey = "" #Current Key (for values that span multiple lines)
        curVal = "" #Current Value (For the same)
        curPath = "" #Path for nested object

        for line in inputFile:
                splitLine = line.split(' ', 1)

                #Found a new path/hierarchy (i.e, there is no right column) or we have the stupid header
                if not len(splitLine) > 1 or "HHHHHT" in splitLine[0]:
                        #this is a new embedding or patent
                        header = splitLine[0].replace('\n','').replace('\r','')

                        #Header of a file, just skip this and move on
                        if "HHHHHT" in header:
                                continue

                        if "PATN" in header: #New Patent

                                #Create our new list, if we have one (avoiding first case of an empty list)
                                if len(patentDictionary) > 0:
                                        patentKey = str(fileName) + str(index) #Index (per file) + timestamp = Unique Identifier

                                        indexDictionary(rServer, patentDictionary, patentKey) #Indexying for querying

                                        rServer.hmset(patentKey, patentDictionary)

                                        #Clear our residual values
                                        patentDictionary = {}
                                        curPath = ""

                                        print "---Stored patent with key", patentKey, "---"
                                        print "---------------NEW PATENT---------------"

                                        index = index + 1

                        else:
                                curPath = header + "/" #New subsection. Assuming max 1 level of embedding

                #Continueing a key or found new key
                else:
                        #If this is a new key
                        if len(splitLine[0]) > 1:

                                #Store old value, if any
                                if len(curKey) > 1:
                                        if 'Date' in curKey:
                                                curVal = convertDateToTimestamp(curVal[1:]) #ignore first space

                                        patentDictionary[curKey] = curVal
                                        curKey = ""
                                        curVal = ""

                                #Start new K:V pair
                                curKey = curPath + splitLine[0]
                                curVal = splitLine[1]

                                #Convert key value to a friendly version, if we care about it (for querying later)
                                curKey = convertTextKey(curKey)

                        #Continuation of a key
                        else:
                                curVal = curVal + splitLine[1]

                        curVal = curVal.replace('    ', '').replace('\n','').replace('\r','') #Strip any unwanted garbage we've collected from the ugly ASCII formatting, yes we want to do this repeatedly

        return index

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

#TODO: Given a hashmap, convert keys
#For now, converts based on hard coded values
#Note these need to be at the base hierarchy - this is on purpose (i.em no sub-elements)
def convertTextKey(key):
        if key == "APD":
                return "ApprovalDate"

        if key == "ISD":
                return "IssueDate"

        if key == "TTL":
                return "Title"

        if "PAR" in key:
                return "Description"

        return key

def convertXMLKey(key):

        if "/wrapper/us-patent-grant" in key and "/us-bibliographic-data-grant/publication-reference/document-id/date" in key:
                return "IssueDate"
        if "/wrapper/us-patent-grant" in key and "/us-bibliographic-data-grant/application-reference/document-id/date" in key:
                return "ApprovalDate"
        if "/wrapper/us-patent-grant" in key and "/us-bibliographic-data-grant/invention-title" in key:
                return "Title"
        if "/wrapper/us-patent-grant" in key and "/claims/claim/claim-text" in key:
                return "Description"

        return key

def convertDateToTimestamp(date):
        year = date[:4].strip()
        month = date[4:6].strip()
        day = date[6:8].strip()

        timeString = day + "/" + month + "/" + year

        total = time.mktime(datetime.datetime.strptime(timeString, "%d/%m/%Y").timetuple())

        return total

def indexDictionary(rServer, dictionary, key):
        #Making ApprovalDate queryable
        if "ApprovalDate" in dictionary:
                #print "Indexed", key, "by approval date", dictionary['ApprovalDate']
                rServer.zadd("ApprovalDate", dictionary["ApprovalDate"], key) #Adds this lists KEY to an ordered set for querying. Rank is the date

        #Making Issued Date queryable
        if "IssueDate" in dictionary:
                #print "Indexed", key, "by issue date", dictionary['IssueDate']
                rServer.zadd("IssueDate", dictionary["IssueDate"], key) #Adds this lists KEY to an ordered set for querying. Rank is the date

        if "Title" in dictionary:
                #print "Indexed", key, "by Title", dictionary['Title']
                rServer.sadd('Title', dictionary['Title'] + ":" + key)

        if "Description" in dictionary:
                #print "Indexed", key, "by Description", dictionary['Description']
                rServer.sadd('Description', dictionary['Description'] + ":" + key)

if __name__ == '__main__':
    main()
