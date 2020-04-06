#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import threading

FIRST_TABLE_NAME = 'MovieRating'
SECOND_TABLE_NAME = 'MovieBoxOfficeCollection'
SORT_COLUMN_NAME_FIRST_TABLE = 'Rating'
SORT_COLUMN_NAME_SECOND_TABLE = 'Collection'
JOIN_COLUMN_NAME_FIRST_TABLE = 'MovieID'
JOIN_COLUMN_NAME_SECOND_TABLE = 'MovieID'

numberOfThreads = 5

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):

    cur = openconnection.cursor()

    #getting min value and max value from the sorting column
    query_str = "SELECT MIN(" + SortingColumnName + "), MAX(" + SortingColumnName + ") FROM " + InputTable + ";"
    cur.execute(query_str)
    minval, maxval = cur.fetchone()

    #creating intervals based on number of threads
    interval = float(maxval - minval) / numberOfThreads

    #getting input table schema to generate partition tables
    query = "SELECT column_name,data_type FROM information_schema.columns WHERE table_name = \'" + InputTable + "\';"
    cur.execute(query)
    InputTableSchema = cur.fetchall()
    #print(InputTableSchema)

    #create "total number of threads" partition tables
    for i in range(numberOfThreads):
        partTable = "range_part" + str(i)
        cur.execute("DROP TABLE IF EXISTS " + partTable)
        temp = "CREATE TABLE " + partTable + " (" + InputTableSchema[0][0] + " " + InputTableSchema[0][1] + ");"
        cur.execute(temp)

        for value in range(1, len(InputTableSchema)):
            temp = "ALTER TABLE " + partTable + " ADD COLUMN " + InputTableSchema[value][0] + " " + InputTableSchema[value][
                1] + ";"
            cur.execute(temp)

    threads = [0]*numberOfThreads

    for i in range(numberOfThreads):

        minvalue = minval + i * interval
        maxvalue = minvalue + interval

        #implementing parallelized threads
        threads[i] = threading.Thread(target=helperSort,
                                      args=(InputTable, SortingColumnName, i, minvalue, maxvalue, openconnection))

        threads[i].start()

    for i in range(numberOfThreads):
        threads[i].join()


    #create output table
    cur.execute("DROP TABLE IF EXISTS " + OutputTable)
    temp = "CREATE TABLE " + OutputTable + " (" + InputTableSchema[0][0] + " " + InputTableSchema[0][1] + ");"
    cur.execute(temp)
    for value in range(1, len(InputTableSchema)):
        temp = "ALTER TABLE " + OutputTable + " ADD COLUMN " + InputTableSchema[value][0] + " " + InputTableSchema[value][
            1] + ";"
        cur.execute(temp)

    #insert data into output table from partition tables
    for i in range(numberOfThreads):
        temp = "INSERT INTO " + OutputTable + " SELECT * FROM range_part" + str(i) + ";"
        cur.execute(temp)

    #delete all the partition tables
    for i in range(numberOfThreads):
        temp = "DROP TABLE IF EXISTS range_part" + str(i) + ";"
        cur.execute(temp)

    openconnection.commit()


def helperSort(InputTable, SortingColumnName, i, minvalue, maxvalue, openconnection):
    cur = openconnection.cursor()
    partTable = "range_part" + str(i)

    #insert data into corresponding partition table
    if i == 0:
        temp = "INSERT INTO " + partTable + " SELECT * FROM " + InputTable + " WHERE " + SortingColumnName + " >= " + str(
            minvalue) + " AND " + SortingColumnName + " <= " + str(
            maxvalue) + " ORDER BY " + SortingColumnName + " ASC;"
    else:
        temp = "INSERT INTO " + partTable + " SELECT * FROM " + InputTable + " WHERE " + SortingColumnName + " > " + str(
            minvalue) + " AND " + SortingColumnName + " <= " + str(
            maxvalue) + " ORDER BY " + SortingColumnName + " ASC;"
    cur.execute(temp)
    return

def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    cur = openconnection.cursor()
    temp = "SELECT MIN(" + Table1JoinColumn + "), MAX(" + Table1JoinColumn + ") FROM " + InputTable1 + ";"
    cur.execute(temp)
    mintbl1,maxtbl1 = cur.fetchone()

    temp = "SELECT MIN(" + Table2JoinColumn + "), MAX(" + Table2JoinColumn + ") FROM " + InputTable2 + ";"
    cur.execute(temp)
    mintbl2,maxtbl2 = cur.fetchone()

    #print(mintbl1,maxtbl1,mintbl2,maxtbl2)

    maxval = max(maxtbl1, maxtbl2)
    minval = min(mintbl1, mintbl2)
    interval = (maxval - minval) / numberOfThreads

    temp = "SELECT column_name,data_type FROM information_schema.columns WHERE table_name = \'" + InputTable1 + "\';"
    cur.execute(temp)
    InputTableSchema1 = cur.fetchall()

    temp = "SELECT column_name,data_type FROM information_schema.columns WHERE table_name = \'" + InputTable2 + "\';"
    cur.execute(temp)
    InputTableSchema2 = cur.fetchall()

    #create schema of output table
    cur.execute("DROP TABLE IF EXISTS " + OutputTable)
    temp = "CREATE TABLE " + OutputTable + " (" + InputTableSchema1[0][0] + " " + InputTableSchema1[0][1] + ");"
    cur.execute(temp)

    for value in range(1, len(InputTableSchema1)):
        temp = "ALTER TABLE " + OutputTable + " ADD COLUMN " + InputTableSchema1[value][0] + " " + \
               InputTableSchema1[value][1] + ";"
        cur.execute(temp)

    for value in range(len(InputTableSchema2)):
        temp = "ALTER TABLE " + OutputTable + " ADD COLUMN " + InputTableSchema2[value][0] + " " + \
               InputTableSchema2[value][1] + ";"
        cur.execute(temp)

    for i in range(numberOfThreads):
        partTable = "parttable1_" + str(i)
        if i == 0:
            minvalue = minval
            maxvalue = minvalue + interval
            temp = "CREATE TABLE " + partTable + " AS SELECT * FROM " + InputTable1 + " WHERE " + Table1JoinColumn + " >= " + str(
                minvalue) + " AND " + Table1JoinColumn + " <= " + str(maxvalue) + ";"
        else:
            minvalue = maxvalue
            maxvalue = minvalue + interval
            temp = "CREATE TABLE " + partTable + " AS SELECT * FROM " + InputTable1 + " WHERE " + Table1JoinColumn + " > " + str(
                minvalue) + " AND " + Table1JoinColumn + " <= " + str(maxvalue) + ";"
        cur.execute(temp)

    for i in range(numberOfThreads):
        partTable = "parttable2_" + str(i)
        if i == 0:
            minvalue = minval
            maxvalue = minvalue + interval
            temp = "CREATE TABLE " + partTable + " AS SELECT * FROM " + InputTable2 + " WHERE " + Table2JoinColumn + " >= " + str(
                minvalue) + " AND " + Table2JoinColumn + " <= " + str(maxvalue) + ";"
        else:
            minvalue = maxvalue
            maxvalue = minvalue + interval
            temp = "CREATE TABLE " + partTable + " AS SELECT * FROM " + InputTable2 + " WHERE " + Table2JoinColumn + " > " + str(
                minvalue) + " AND " + Table2JoinColumn + " <= " + str(maxvalue) + ";"
        cur.execute(temp)

    #create output partition tables to store data by joining input partition tables
    for i in range(numberOfThreads):
        OutputTableRange = "outparttable_" + str(i)
        temp = "CREATE TABLE " + OutputTableRange + " (" + InputTableSchema1[0][0] + " " + InputTableSchema1[0][1] + ");"
        cur.execute(temp)

        for value in range(1, len(InputTableSchema1)):
            temp = "ALTER TABLE " + OutputTableRange + " ADD COLUMN " + InputTableSchema1[value][0] + " " + \
                   InputTableSchema1[value][1] + ";"
            cur.execute(temp)

        for value in range(len(InputTableSchema2)):
            temp = "ALTER TABLE " + OutputTableRange + " ADD COLUMN " + InputTableSchema2[value][0] + " " + \
                   InputTableSchema2[value][1] + ";"
            cur.execute(temp)

    threads = [0]*numberOfThreads
    for i in range(numberOfThreads):
        threads[i] = threading.Thread(target=helperJoin, args=(Table1JoinColumn, Table2JoinColumn, openconnection, i))
        threads[i].start()

    for i in range(numberOfThreads):
        threads[i].join()

    #insert data into main output table from all the output partition tables
    for i in range(numberOfThreads):
        temp = "INSERT INTO " + OutputTable + " SELECT * FROM outparttable_" + str(i) + ";"
        cur.execute(temp)

    #drop all the partition tables
    for i in range(numberOfThreads):
        drop1 = "DROP TABLE IF EXISTS parttable1_" + str(i) + ";"
        drop2 = "DROP TABLE IF EXISTS parttable2_" + str(i) + ";"
        outdrop = "DROP TABLE IF EXISTS outparttable_" + str(i) + ";"
        cur.execute(drop1)
        cur.execute(drop2)
        cur.execute(outdrop)

    openconnection.commit()


def helperJoin(Table1JoinColumn, Table2JoinColumn, openconnection, i):
    cur = openconnection.cursor()

    #insert data into corresponding output partition table by joining two input partition tables
    temp = """INSERT INTO outparttable_""" + str(i) + """ SELECT * FROM parttable1_""" + str(
        i) + """ INNER JOIN parttable2_""" + str(i) + """ ON parttable1_""" + str(i) + """.""" + str(
        Table1JoinColumn).lower() + """ = parttable2_""" + str(i) + """.""" + str(Table2JoinColumn).lower() + """;"""
    cur.execute(temp)
    return
