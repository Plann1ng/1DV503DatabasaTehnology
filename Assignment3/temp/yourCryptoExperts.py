import mysql.connector
from mysql.connector import errorcode
import os
import msvcrt as m
import csv
from tkinter import *


# Open the csv file.
cnx = mysql.connector.connect(user='root',
                             password='99787836',
                             host='localhost'
)

# six readers for six files.
file = open(os.getcwd() + "\\" + "Transactions400x.csv")
csvreader = csv.reader(file)
file2 = open(os.getcwd() + "\\" + "OfficesTableFixed.csv")
csvreader2 = csv.reader(file2)
file3 = open(os.getcwd() + "\\" + "wallets.csv")
csvreader3 = csv.reader(file3)
file4 = open(os.getcwd() + "\\" + "customer.csv")
csvreader4 = csv.reader(file4)
file5 = open(os.getcwd() + "\\" + "taxDetails.csv")
csvreader5 = csv.reader(file5)
file6 = open(os.getcwd() + "\\" + "EmployeeTablex.csv")
csvreader6 = csv.reader(file6)

# Database name
name = 'YourCryptoExperts'
cursor = cnx.cursor()

# Create database with the given name
def create_database(cursor, name):
    try:
        cursor.execute("CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(name))
        print("Using database {}".format(name))
    except mysql.connector.Error as err:
        print("Failed to create database {}".format(err))
        exit(1)

# Use the database
def ifExists(name):
  try:
    cursor.execute("USE {}".format(name))
  except mysql.connector.Error as err:
    print("Database {} does not exist".format(name))
    if err.errno == errorcode.ER_BAD_DB_ERROR:
        create_database(cursor, name)
        print("Database {} created succesfully.".format(name))
        cnx.database = name
    else:
        print(err)

# Default table creation
def create_table_employees(cursor):
    data = "CREATE TABLE `employees` (" \
                 "  `employeeID` int(4) NOT NULL," \
                 "  `office` int(5) NOT NULL," \
                 "  `first_name` varchar(30) NOT NULL," \
                 "  `last_name` varchar(30) NOT NULL," \
                 "  `email` varchar(200) NOT NULL," \
                 "  PRIMARY KEY (`employeeID`)" \
                 ") ENGINE=InnoDB"
    try:
        print("Creating table employees: ")
        cursor.execute(data)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")

# Default table creation
def create_table_customers(cursor):
    data = "CREATE TABLE `customers` (" \
                 "  `id` BIGINT(20) NOT NULL," \
                 "  `first_name` varchar(30) NOT NULL," \
                 "  `last_name` varchar(30) NOT NULL," \
                 "  `caseAgent` int(4) NOT NULL," \
                 "  `walletAdress` varchar(255) NOT NULL," \
                 "  `OCR` int(10) NOT NULL," \
                 "  `email` varchar(200)," \
                 "  PRIMARY KEY (`id`)" \
                 ") ENGINE=InnoDB"
    try:
        print("Creating table customers: ")
        cursor.execute(data)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")

# Default table creation
def create_table_offices(cursor):
    data = "CREATE TABLE `offices` (" \
                 "  `officeCode` int(5) NOT NULL," \
                 "  `email` varchar(200)," \
                 "  `adress` varchar(255)," \
                 "  `phone` BIGINT(11)," \
                 "  PRIMARY KEY (`officeCode`)" \
                 ") ENGINE=InnoDB"
    try:
        print("Creating table offices: ")
        cursor.execute(data)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")

# Default table creation
def create_table_taxDetails(cursor):
    data = "CREATE TABLE `taxDetails` (" \
                 "  `taxID` int(6) NOT NULL," \
                 "  `citizenID` BIGINT(10)," \
                 "  `adress` varchar(255)," \
                 "  `totalProfit` double(10,5)," \
                 "  `taxAmount` double(10,5)," \
                 "  PRIMARY KEY (`taxID`)" \
                 ") ENGINE=InnoDB"
    try:
        print("Creating table taxDetails: ")
        cursor.execute(data)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")

# Default table creation
def create_table_transactions(cursor):
    data = "CREATE TABLE `transactions` (" \
                 "  `transactionID` varchar(255) NOT NULL," \
                 "  `transactionWallet` varchar(255) NOT NULL," \
                 "  `dateWirthdraw` varchar(15)," \
                 "  `heldMonths` int(2)," \
                 "  `buyAmount` double(10,5) NOT NULL," \
                 "  `realizedProfit` varchar(5) NOT NULL," \
                 "  `totalAmount` double(10,5) NOT NULL," \
                 "  `profitAmount` double(10,5)," \
                 "  `method` varchar(20)," \
                 "  PRIMARY KEY (`transactionID`)" \
                 ") ENGINE=InnoDB"
    try:
        print("Creating table transactions: ")
        cursor.execute(data)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")
        
# Default table creation
def create_table_customerWallets(cursor):
    data = "CREATE TABLE `customerWallets` (" \
                 "  `adress` varchar(255) NOT NULL," \
                 "  `profitPerWallet` double(10,5) NOT NULL," \
                 "  PRIMARY KEY (`adress`)" \
                 ") ENGINE=InnoDB"
    try:
        print("Creating table customerWallets: ")
        cursor.execute(data)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")


