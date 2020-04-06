#!/usr/bin/python2.7
#
# Interface for the assignement
#
import sys

import psycopg2

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    #get cur object to work with sql queries
    cur = openconnection.cursor()

    #drop table if already exists
    cur.execute("DROP TABLE IF EXISTS " + ratingstablename)

    #create table and copy data from ratingsfile
    cur.execute(
        "CREATE TABLE " + ratingstablename + " (UserID INT, temp1 VARCHAR(10),  MovieID INT , temp3 VARCHAR(10),  Rating REAL, temp5 VARCHAR(10), Timestamp INT)")
    loaddatafile = open(ratingsfilepath, 'r')
    cur.copy_from(loaddatafile, ratingstablename, sep=':',
                  columns=('UserID', 'temp1', 'MovieID', 'temp3', 'Rating', 'temp5', 'Timestamp'))

    #drop temporary columns
    cur.execute(
        "ALTER TABLE " + ratingstablename + " DROP COLUMN temp1, DROP COLUMN temp3,DROP COLUMN temp5, DROP COLUMN Timestamp")

    #close connection
    cur.close()
    openconnection.commit()


def rangePartition(ratingstablename, numberofpartitions, openconnection):

    #create prefix for name of partition tables
    name = 'range_part'
    try:
        #get cursor object to work with sql queries
        cursor = openconnection.cursor()
        cursor.execute("select * from information_schema.tables where table_name='%s'" % ratingstablename)

        #check if ratings table has data filled in it or not
        if not bool(cursor.rowcount):
            print "Please Load Ratings Table first!!!"
            return
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS RangeRatingsMetadata(PartitionNum INT, MinRating REAL, MaxRating REAL)")
        MinRating = 0.0
        MaxRating = 5.0
        step = (MaxRating - MinRating) / (float)(numberofpartitions)
        i = 0;
        while i < numberofpartitions:
            newTableName = name + `i`
            cursor.execute("CREATE TABLE IF NOT EXISTS %s(UserID INT, MovieID INT, Rating REAL)" % (newTableName))
            i += 1;

        i = 0;
        while MinRating < MaxRating:
            lowerLimit = MinRating
            upperLimit = MinRating + step
            if lowerLimit < 0:
                lowerLimit = 0.0

            if lowerLimit == 0.0:
                cursor.execute(
                    "SELECT * FROM %s WHERE Rating >= %f AND Rating <= %f" % (ratingstablename, lowerLimit, upperLimit))
                rows = cursor.fetchall()
                newTableName = name + `i`
                for row in rows:
                    cursor.execute("INSERT INTO %s(UserID, MovieID, Rating) VALUES(%d, %d, %f)" % (
                        newTableName, row[0], row[1], row[2]))

            if lowerLimit != 0.0:
                cursor.execute(
                    "SELECT * FROM %s WHERE Rating > %f AND Rating <= %f" % (ratingstablename, lowerLimit, upperLimit))
                rows = cursor.fetchall()
                newTableName = name + `i`
                for row in rows:
                    cursor.execute("INSERT INTO %s(UserID, MovieID, Rating) VALUES(%d, %d, %f)" % (
                        newTableName, row[0], row[1], row[2]))
            cursor.execute(
                "INSERT INTO RangeRatingsMetadata (PartitionNum, MinRating, MaxRating) VALUES(%d, %f, %f)" % (
                    i, lowerLimit, upperLimit))
            MinRating = upperLimit
            i += 1;

        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):

    #create table names for partitions
    prefix = "rrobin_part"
    partitions = []
    for i in range(0, numberofpartitions):
        temp = prefix + str(i)
        partitions.append(temp)


    #iterate through all records and insert them into corresponding partition table using round robin manner
    for i in range(0, numberofpartitions):
        # Create the partition table
        command = """CREATE TABLE """ + str(partitions[i]) + """ (userid INTEGER,movieid INTEGER,rating REAL);"""
        cur = openconnection.cursor()
        cur.execute(command)
        openconnection.commit()

        # Find the rowid of the records and take the mod value with num of partitions and insert in the required partition
        command = """INSERT INTO """ + str(partitions[
                                               i]) + """ (userid,movieid,rating) SELECT userid,movieid,rating FROM (SELECT row_number() over(), * FROM """ + ratingstablename + """ ) AS temp WHERE  (row_number - 1)%""" + str(
            numberofpartitions) + """ = """ + str(i) + """;"""
        cur.execute(command)
        openconnection.commit()


    openconnection.commit()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):

    #get cur object to work with sql queries
    con = openconnection
    cur = con.cursor()

    #create prefix for partition table name
    RROBIN_TABLE_PREFIX = 'rrobin_part'

    #insert record into original ratings table
    cur.execute("insert into " + ratingstablename + "(userid, movieid, rating) values (" + str(userid) + "," + str(
        itemid) + "," + str(rating) + ");")

    #get total number of rows after inserting above record
    cur.execute("select count(*) from " + ratingstablename + ";");
    total_rows = (cur.fetchall())[0][0]

    #get total number of partition
    numberofpartitions = count_partitions(RROBIN_TABLE_PREFIX, openconnection)

    #get an index of particular partition table to insert the record into
    index = (total_rows - 1) % numberofpartitions

    #get name of partition table and insert record into it
    table_name = RROBIN_TABLE_PREFIX + str(index)
    cur.execute("insert into " + table_name + "(userid, movieid, rating) values (" + str(userid) + "," + str(
        itemid) + "," + str(rating) + ");")
    cur.close()
    con.commit()



def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):

    #get cursor object to work with sql queries
    con = openconnection
    cur = con.cursor()

    #get prefix for partition tables
    RANGE_TABLE_PREFIX = 'range_part'

    #get total number of partitions
    numberofpartitions = count_partitions(RANGE_TABLE_PREFIX, openconnection)

    delta = 5 / numberofpartitions
    index = int(rating / delta)
    if rating % delta == 0 and index != 0:
        index = index - 1

    #get particular table name to insert the record into
    table_name = RANGE_TABLE_PREFIX + str(index)

    #insert record
    cur.execute("insert into " + table_name + "(userid, movieid, rating) values (" + str(userid) + "," + str(
        itemid) + "," + str(rating) + ");")

    cur.close()
    con.commit()


def createDB(dbname='dds_assignment'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    finally:
        if cursor:
            cursor.close()

#function to count total number of partitions
def count_partitions(prefix, openconnection):
    con = openconnection
    cur = con.cursor()
    cur.execute("select count(*) from pg_stat_user_tables where relname like " + "'" + prefix + "%';")
    count = cur.fetchone()[0]
    cur.close()

    return count