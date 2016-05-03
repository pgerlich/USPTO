import sys
import redis
import time
import datetime
import subprocess
import random
import zipfile


def main():
	try:
		dateRange = str(sys.argv[1]) #First input is field (Title, IssueDate, ApprovalDate, Description)
	except IndexError:
		print "Incorret syntax. Expecting python bulk_download.py xxxx-xxxx"
		return
	
	minRange = dateRange.split('-')[0]
	maxRange = dateRange.split('-')[1]

	if ( minRange == '*' ):
		minYearVal = 1970
		minMonthVal = 01
		minDayVal = 01
	else:
		minYearVal = minRange[:4]
		minMonthVal = minRange[4:6]
		minDayVal = minRange[6:8]

	if ( maxRange == '*' ):
		maxYearVal = "2050"
		maxMonthVal = "12"
		maxDayVal = "31"
	else:
		maxYearVal = maxRange[:4]
                maxMonthVal = maxRange[4:6]
                maxDayVal = maxRange[6:8]

	print 'Start year:' , minYearVal, minMonthVal, minDayVal
	print 'End year:', maxYearVal, maxMonthVal, maxDayVal


	downloadRange(minYearVal, minMonthVal, minDayVal, maxYearVal, maxMonthVal, maxDayVal)

def downloadRange(startYear, startMonth, startDay, endYear, endMonth, endDay):
	startTimestamp = findNearestTuesday(startYear, startMonth, startDay)
	endTimestamp = findNearestTuesday(endYear, endMonth, endDay)

	curTimestamp = startTimestamp

	while curTimestamp < endTimestamp:
		curDayTuple = time.gmtime(curTimestamp) #Current day time tuple

		curYear = str(curDayTuple[0])
		curMonth = str(curDayTuple[1])
		curDay = str(curDayTuple[2])
		curWeek = str((curDayTuple[7] / 7) + 1)

		if int(curMonth) < 10:
			curMonth = str('0') + str(curMonth)

		if int(curDay) < 10:
			curDay = str('0') + str(curDay)

		if int(curWeek) < 10:
			curWeek = str('0') + str(curWeek)
		
		print curYear, curMonth, curDay, curWeek

		if int(curYear) < 2002:
			filePath = "pftaps" + curYear + curMonth + curDay + "_wk" + curWeek + ".zip"
			fileURL = "http://storage.googleapis.com/patents/grant_full_text/" + curYear + '/' + filePath
		elif int(curYear) > 2002 and int(curYear) < 2005:
			filePath = "pg" + curYear[2:] + curMonth + curDay + ".zip"
			fileURL = "http://storage.googleapis.com/patents/grant_full_text/" + curYear + '/' + filePath
		elif int(curYear) >= 2005:
			filePath = "ipg" + curYear[2:] + curMonth + curDay + ".zip"
			fileURL = "http://storage.googleapis.com/patents/grant_full_text/" + curYear + '/' + filePath
	
		try:
			subprocess.check_output(["wget", fileURL])
			zfile = zipfile.ZipFile(filePath)
			zfile.extractall("data")
		except:
			print fileURL + " not found. File: " + filePath
		
		curTimestamp = curTimestamp + 604800 # Next week

		#Psuedoranom timeout
		timeoutDuration = random.randint(15,30)
		print "Sleeping for:", timeoutDuration , 'seconds'
		time.sleep(timeoutDuration)

def findNearestTuesday(year, month, day):
	timestampForDate = dateToTimestamp(year, month, day)

	daysAwayFromTuesday = 1 - int(dateToTimestruct(year, month, day)[6]) #Get number of days difference from Tuesday
	secondsAwayFromTuesday = 86400 * daysAwayFromTuesday #MS difference to Tuesday

	return (timestampForDate + secondsAwayFromTuesday)

def dateToTimestamp(year, month, day):
	timeString = str(day) + "/" + str(month) + "/" + str(year)

	total = time.mktime(datetime.datetime.strptime(timeString, "%d/%m/%Y").timetuple())

	return total

def dateToTimestruct(year, month, day):
	timeString = str(day) + "/" + str(month) + "/" + str(year)

	return datetime.datetime.strptime(timeString, "%d/%m/%Y").timetuple()


if __name__ == '__main__':
    main()
