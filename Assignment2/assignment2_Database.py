import mysql.connector
from mysql.connector import errorcode
import os
import msvcrt as m
import re
import string
import csv

cnx = mysql.connector.connect(user='root',
                             password='99787836',
                             host='localhost'
)

name = 'Shaban'
cursor = cnx.cursor()

# NOTE Make sure that your files are on the current working directory path, If not please consider changing it.
# Put species here
file = open(os.getcwd() + "\\" + "species.csv")
csvreader = csv.reader(file)
# Put planets here
file2 = open(os.getcwd() + "\\" + "planets.csv")
csvreader2 = csv.reader(file2)

# Create database with rs223fx last name.
def create_database(cursor, name):
    try:
        cursor.execute("CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(name))
        print("Using database {}".format(name))
    except mysql.connector.Error as err:
        print("Failed to create database {}".format(err))
        exit(1)

# Check if the database exists
def ifExists(name):
    # if exists use the given database
  try:
    cursor.execute("USE {}".format(name))
    # if not existing rollback and create the database with specified name
  except mysql.connector.Error as err:
    print("Database {} does not exist".format(name))
    if err.errno == errorcode.ER_BAD_DB_ERROR:
        create_database(cursor, name)
        print("Database {} created succesfully.".format(name))
        cnx.database = name
    else:
        print(err)

# Create empty table for planets and define variables.
def create_table_planets(cursor):
    data = "CREATE TABLE `planets` (" \
                 "  `name` varchar(20) NOT NULL," \
                 "  `rotation_period` int(2) NOT NULL," \
                 "  `orbital_period` int(4) NOT NULL," \
                 "  `diameter` int(6) NOT NULL," \
                 "  `climate` varchar(25) NOT NULL," \
                 "  `gravity` varchar(38) NOT NULL," \
                 "  `terrain` varchar(50) NOT NULL," \
                 "  `surface_water` int(4) NOT NULL," \
                 "  `population` BIGINT(255) NOT NULL," \
                 "  PRIMARY KEY (`name`)" \
                 ") ENGINE=InnoDB"
    try:
        print("Creating table planets: ")
        cursor.execute(data)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")

# Create empty table for species and define variables.
def create_table_species(cursor):
    data = "CREATE TABLE `species` (" \
                 "  `name` varchar(255) NOT NULL," \
                 "  `classification` varchar(255)," \
                 "  `designation` varchar(255) NOT NULL," \
                 "  `average_height` int(3)," \
                 "  `skin_colors` varchar(255)," \
                 "  `hair_colors` varchar(255)," \
                 "  `eye_colors` varchar(255)," \
                 "  `average_lifespan` int(3)," \
                 "  `language` varchar(255)," \
                 "  `homeworld` varchar(250)," \
                 "  PRIMARY KEY (`name`)" \
                 ") ENGINE=InnoDB"
    try:
        print("Creating table species: ")
        cursor.execute(data)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")

# Retrieve information from species.csv and import to the database.
def insert_into_species(cursor, file):
    # Iterate over all the values individually
    for name, two, three, four, five, six, seven, eight, nine, ten in csvreader:
        # Replace the values that are broking the import
        if isinstance(nine, str):
            if "'" in nine:
                    nine = "Twileki"
        # special case for indefinite
        if isinstance(eight, str):
                if "in" in eight:
                        eight = 0
        if two == "NA":
                two = "NULL"
        if three == "NA":
                three = "NULL"
        if five == "NA":
                five = "NULL"
        if six == "NA":
                six = "NULL"
        if seven == "NA":
                seven = "NULL"
        if eight == "NA":
                eight = "NULL"
        if nine == "NA":
                nine = "NULL"
        if ten == "NA":
                ten = "NULL"
        if four == "NA":
            four = 000
        elif "'" in name:
                name = name.replace("'", " ")
        elif "'" in nine:
                nine = nine.replace("'", " ")
        elif (eight == "NA"):
                eight = 000

        # Insert each row and commit for each row
        values = "('{:<50}', '{:<50}', '{:<50}', {:<50}, '{:<50}', '{:<50}', '{:<50}', {:<50}, '{:<50}', '{:<50}');".format(name, two, three, four, five, six, seven, eight, nine, ten)
        cursor.execute("INSERT INTO species VALUES {}".format(values))
        cnx.commit()

# Retrieve information from planets.csv and import to the database.
def insert_into_planets(cursor, file):
    # Replace the values that are broking the import
    for one, two, three, four, five, six, seven, eight, nine in csvreader2:
        # remove the values where primary key not exists
        if one == "NA":
          continue

        if four == "NA":
            four = 0
        if two == "NA":
                two = 0
        if three == "NA":
                three = 0
        if five == "NA":
                five = "None"
        if six == "NA":
                six = "None"
        if seven == "NA":
                seven = "None"
        if eight == "NA":
                eight = 0
        if nine == "NA":
                nine = 0
        # import and commit the data
        values = "('{}', {}, {}, {}, '{}', '{}', '{}', {}, {});".format(one, two, three, four, five, six, seven, eight, nine)
        cursor.execute("INSERT INTO planets VALUES {:<1}".format(values))
        cnx.commit()
    
# NOTE Keep all the functions open for the first iteration, however after the data was retrieved please consider just adding "#"
# on line 175, 177, 178, 179, 180.

create_database(cursor, name)
ifExists(name)
create_table_planets(cursor)
create_table_species(cursor)
insert_into_species(cursor, file)
insert_into_planets(cursor, file)

#pause method
def wait():
    m.getch()

# Database-like printing
def clearPrint(cursor):
    print("\nFetched results: ")
    print("+{}+".format("-"*52))

    for i in cursor:
        for name in i:
            if name == "NA" or name == "none":
                continue
            else: 
                print("|{:<52}|".format(name))  # *row == row element wise
                print("+{}+".format("-"*52))
