iimport sys
import redis
import time
import datetime
import getopt

def main():
        try:
                opts, args = getopt.getopt(sys.argv[1:],'i', ['WipeMe=', 'ApprovalDate=', 'IssueDate=', 'Title=', 'Description='])

                wipeMe = ""
                approvalDate = ""
                issueDate = ""
                title = ""
                description = ""

                for o, a in opts:
                        if o == '--ApprovalDate':
                                approvalDate = a
                        if o == '--IssueDate':
                                issueDate = a
                        if o == '--Title':
                                title = a
                        if o == '--Description':
                                description = a
                        if o == '--WipeMe':
                                wipeMe = True

        except getopt.GetoptError:
                print 'query.py --ApprovalDate=*-19700101 --title="computer writing" --description="art tool"'
                sys.exit(2)

        #Connect to redis server
        rServer = redis.StrictRedis(host='localhost', port=6379, db=0)#NOTE: Using DB 0

        finalResult  = None

        if len(title) > 0:
                finalResult = queryByTitle(rServer, title)

        if len(approvalDate) > 0:
                if finalResult == None:
                        finalResult = queryByApprovalDate(rServer, approvalDate)
                elif len(finalResult) > 0:
                        approvalDateMatches = queryByApprovalDate(rServer, approvalDate)
                        if approvalDateMatches is not None:
                                finalResult = finalResult & approvalDateMatches

        if len(issueDate) > 0:
                if finalResult == None:
                        finalResult = finalResult & queryByIssueDate(rServer, issueDate)
                elif len(finalResult) > 0:
                        issueDateMatches = queryByIssueDate(rServer,issueDate)
                        if issueDateMatches is not None:
                                finalResult = finalResult & issueDateMatches

        if len(description) > 0:
                if finalResult == None:
                        finalResult = queryByDescription(rServer, description)
                elif len(finalResult) > 0:
                        descriptionMatches = queryByDescription(rServer, description)
                        if descriptionMatches is not None:
                                finalResult = finalResult & descriptionMatches

        if wipeMe == True:
                wipeRedisDB(rServer)
        else:
                printQueryResults(rServer, finalResult)


def printQueryResults(rServer, results):
        sys.stderr.write("Results: " + str(len(results)) + '\n')

        for result in results:
                print "-------------"
                print "Key:" , result
                print "Title:", str(rServer.hmget(result, ["Title"]))[2:-2]

                descriptionWordArray = str(rServer.hmget(result, ["Description"])).split(' ')
                descriptionText = ""
                for word in descriptionWordArray[:100]:
                        descriptionText = descriptionText + word + ' '

                if len(descriptionWordArray) > 100:
                        descriptionText = descriptionText + "..."

                print "Description:" , descriptionText[2:-3]
                print "Approved:" , str(convertTimestampToDate(rServer.hget(result, 'ApprovalDate')))
                print "Issued:" , str(convertTimestampToDate(rServer.hget(result, 'IssueDate')))
                print "-------------"

#Confirmed, functions as expected
def queryByApprovalDate(rServer, query):
        if ( query.split('-')[0] == '*' ):
                minVal = 0
        else:
                minVal = convertDateToTimestamp(query.split('-')[0])

        if ( query.split('-')[1] == '*' ):
                maxVal = time.time()
        else:
                maxVal = convertDateToTimestamp(query.split('-')[1])

        matches = rServer.zrangebyscore('ApprovalDate', minVal, maxVal)

        return set(matches)

        for match in matches:
                print convertTimestampToDate(rServer.hget(match, 'ApprovalDate')) + "," + rServer.hget(match, 'Title')

#Confirmed, functions as expected
def queryByIssueDate(rServer, query):
        if ( query.split('-')[0] == '*' ):
                minVal = 0
        else:
                minVal = convertDateToTimestamp(query.split('-')[0])

        if ( query.split('-')[1] == '*' ):
                maxVal = time.time()
        else:
                maxVal = convertDateToTimestamp(query.split('-')[1])

        matches = rServer.zrangebyscore('IssueDate', minVal, maxVal)

        return set(matches)

#Confirmed, functions as expected
def queryByTitle(rServer, query):
        keys = set()

        query = query.replace('"', '').split(" ")

        for title in rServer.sscan_iter('Title'):
                numberOfQueryWordMatches = 0

                #Case insensitive title string matching. Niave implementation
                for q in query:
                        for t in title.split(" "):
                                if q.lower() == t.lower():
                                        numberOfQueryWordMatches = numberOfQueryWordMatches + 1

                                if numberOfQueryWordMatches == len(query):
                                        keys.add(title.split(':')[1])

        return keys

#Confirmed, functions as expected
def queryByDescription(rServer, query):
        keys = set()

        query = query.replace('"', '').split(" ")

        for description in rServer.sscan_iter('Description'):
                numberOfQueryWordMatches = 0

                #Case insensitive description string matching. Niave implementation
                for q in query:
                        for t in description.split(" "):
                                if q.lower() == t.lower():
                                        numberOfQueryWordMatches = numberOfQueryWordMatches + 1

                                if numberOfQueryWordMatches == len(query):
                                        keys.add(description.split(':')[1])

        return keys

#Confirmed, functions as expected
def wipeRedisDB(rServer):
        print "Deleting all data in database"
        rServer.flushall()

#Confirmed, functions as expected
def convertDateToTimestamp(date):
        year = date[:4].strip()
        month = date[4:6].strip()
        day = date[6:8].strip()

        timeString = day + "/" + month + "/" + year

        total = time.mktime(datetime.datetime.strptime(timeString, "%d/%m/%Y").timetuple())

        return total

def convertTimestampToDate(timestamp):
        if type(timestamp) is float or type(timestamp) is str:
                return datetime.datetime.fromtimestamp(float(timestamp)).strftime('%Y-%m-%d %H:%M:%S')

if __name__ == '__main__':
    main()

