#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    rangeratingprefix = 'rangeratingspart'
    roundrobinratingprefix = 'roundrobinratingspart'

    file = open("RangeQueryOut.txt", "w+")

    cur = openconnection.cursor()
    command = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE '" + rangeratingprefix + "%';"
    cur.execute(command)

    openconnection.commit()
    noOfRangePartition = cur.fetchone()[0]

    for i in range(0, noOfRangePartition):
        part = rangeratingprefix + str(i)
        command = "SELECT * FROM " + part + " WHERE Rating >= " + str(ratingMinValue) + " AND Rating <= " + str(
            ratingMaxValue) + ";"
        cur.execute(command)
        openconnection.commit()
        allTuples = cur.fetchall()
        ConvertedPart = 'RangeRatingsPart'+str(i)
        for eachRecord in allTuples:
            temp = str(ConvertedPart) + ',' + str(eachRecord[0]) + ',' + str(eachRecord[1]) + ',' + str(eachRecord[2]) + '\n'
            file.write(temp)

    command = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE '" + roundrobinratingprefix + "%';"
    cur.execute(command)

    openconnection.commit()
    noOfRoundRobinPartition = cur.fetchone()[0]

    for i in range(0, noOfRoundRobinPartition):
        part = roundrobinratingprefix + str(i)
        command = "SELECT * FROM " + part + " WHERE Rating >= " + str(ratingMinValue) + " AND Rating <= " + str(
            ratingMaxValue) + ";"
        cur.execute(command)
        openconnection.commit()
        allTuples = cur.fetchall()
        ConvertedPart = 'RoundRobinRatingsPart'+str(i)
        for eachRecord in allTuples:
            temp = str(ConvertedPart) + ',' + str(eachRecord[0]) + ',' + str(eachRecord[1]) + ',' + str(eachRecord[2]) + '\n'
            file.write(temp)

    file.close()



def PointQuery(ratingsTableName, ratingValue, openconnection):
    rangeratingprefix = 'rangeratingspart'
    roundrobinratingprefix = 'roundrobinratingspart'

    file = open("PointQueryOut.txt", "w+")

    cur = openconnection.cursor()
    command = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE '" + rangeratingprefix + "%';"
    cur.execute(command)

    openconnection.commit()
    noOfRangePartition = cur.fetchone()[0]

    for i in range(0, noOfRangePartition):
        part = rangeratingprefix + str(i)
        command = "SELECT * FROM " + part + " WHERE Rating = " + str(ratingValue) + ";"
        cur.execute(command)
        openconnection.commit()
        allTuples = cur.fetchall()
        ConvertedPart = 'RangeRatingsPart' + str(i)
        for eachRecord in allTuples:
            temp = str(ConvertedPart) + ',' + str(eachRecord[0]) + ',' + str(eachRecord[1]) + ',' + str(eachRecord[2]) + '\n'
            file.write(temp)

    command = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE '" + roundrobinratingprefix + "%';"
    cur.execute(command)

    openconnection.commit()
    noOfRoundRobinPartition = cur.fetchone()[0]

    for i in range(0, noOfRoundRobinPartition):
        part = roundrobinratingprefix + str(i)
        command = "SELECT * FROM " + part + " WHERE Rating = " + str(ratingValue) + ";"
        cur.execute(command)
        openconnection.commit()
        allTuples = cur.fetchall()
        ConvertedPart = 'RoundRobinRatingsPart' + str(i)
        for eachRecord in allTuples:
            temp = str(ConvertedPart) + ',' + str(eachRecord[0]) + ',' + str(eachRecord[1]) + ',' + str(eachRecord[2]) + '\n'
            file.write(temp)

    file.close()


def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()
