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
