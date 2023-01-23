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

# Inserting to the table transaction details.
def insert_into_transactions(cursor, file):
    for name, two, three, four, five, six, seven, eight, nine in csvreader:
        values = "('{:<1}', '{:<1}', '{:<1}', {:<1}, {:<1}, {:<1}, {:<1}, {:<1}, '{:<1}');".format(name, two, three, four, five, six, seven, eight, nine)
        cursor.execute("INSERT INTO transactions VALUES {}".format(values))
        cnx.commit()
 
 # Inserting to offices necesarry information.
def insert_into_offices(cursor, file2):
    for name, two, three, four in csvreader2:
        values = "({:<1}, '{:<1}', '{:<1}', {:<1});".format(name, two, three, four)
        cursor.execute("INSERT INTO offices VALUES {}".format(values))
        cnx.commit()

# Inserting into wallets values
def insert_into_wallets(cursor, file3):
    for name, two in csvreader3:
        values = "('{:<1}', {:<1});".format(name, two)
        cursor.execute("INSERT INTO customerWallets VALUES {}".format(values))
        cnx.commit()
        
# Inserting into customers table values
def insert_into_customers(cursor, file4):
    for name, two, three, four, five, six, seven in csvreader4:
        values = "({:<1}, '{:<1}', '{:<1}', {:<1}, '{:<1}', {:<1}, '{:<1}');".format(name, two, three, four, five, six, seven)
        cursor.execute("INSERT INTO customers VALUES {}".format(values))
        cnx.commit()

# Inserting into taxDetails necesarry values
def insert_into_taxDetails(cursor, file5):
    for name, two, three, four, five in csvreader5:
        values = "({:<1}, {:<1}, '{:<1}', {:<1}, {:<1});".format(name, two, three, four, five)
        cursor.execute("INSERT INTO taxDetails VALUES {}".format(values))
        cnx.commit()

# Inserting into employees
def insert_into_employees(cursor, file6):
    for name, two, three, four, five in csvreader6:
        values = "({:<1}, {:<1}, '{:<1}', '{:<1}', '{:<1}');".format(name, two, three, four, five)
        cursor.execute("INSERT INTO employees VALUES {}".format(values))
        cnx.commit()

# NOTE !! Each function further have its own tkinter window.


# Average tax paid within 2020-2021
# Motive included that query, because even though simple it is important for data statistics.
def averageTaxPaid():
    cursor.execute("SELECT avg(taxAmount) as AveragetaxPaid from taxDetails;")
    result = cursor.fetchall()
    for i in result:
        for letter in i:
            if letter == "(" or letter == "," or letter == ")":
                letter = ''
    new = Tk()
    label1 = Label(new, text="Average tax paid between 2020 - 2021: ${}".format(letter), font="40")
    label1.grid(row=0, column=0)
    canvas = Canvas(new, width=500, height=30)
    canvas.grid(columnspan = 1, rowspan = 6)


# Checking how many profitable transactions given customer managed to perform
def transactionsForCustomer():
    cursor.execute("SELECT customers.first_name as Customer,"\
                   "count(transactionID) as transactionMade,"\
                   "customers.walletAdress as Wallet "\
                   "from customers "\
                   "join transactions on customers.walletAdress = transactions.transactionWallet "\
                   "where customers.ID = 9582113310 "\
                    "and profitAmount > 0;")
    for row, two, three in cursor:
      new = Tk()
      label1 = Label(new, text="How many profitable transactions given customer have:\n\n |NAME: {}\n|PROFITABLE TRANSACTIONS: {}\n|WALLET {}\n".format(row, two, three), font="30")
      label1.grid(row=0, column=0)
      canvas = Canvas(new, width=500, height=30)
      canvas.grid(columnspan = 1, rowspan = 6)

    
# Highest paid tax per wallet
def highestTaxPaid():
    cursor.execute('SELECT CONCAT(customers.first_name," ",'\
                'customers.last_name) as Customer, '\
                'customers.ID as ID, max(taxAmount) as MaximumTaxPaidFor2021 '\
                'from taxDetails '\
                'join customers on taxdetails.citizenID = customers.ID '\
                'join transactions on customers.walletAdress = transactions.transactionWallet '\
                'where dateWirthdraw like "%2021"; ')
    for row, two, three in cursor:
      new = Tk()
      label1 = Label(new, text="Highest tax paid within 2021:\n\n |CUSTOMER : {}\n|ID: {}\n|TAX PAID ${}\n".format(row, two, three), font="30")
      label1.grid(row=0, column=0)
      canvas = Canvas(new, width=500, height=30)
      canvas.grid(columnspan = 1, rowspan = 6)


# Those who held less than 4 months and were profitable.
def heldForLessThanFourMonths():
    cursor.execute("SELECT COUNT(transactions.transactionID) "\
                  "as TransactionCount "\
                  "from transactions "\
                  "where realizedProfit = 1 "\
                  "and heldMonths < 4;")
    result = cursor.fetchall()
    for i in result:
        for letter in i:
            if letter == "(" or letter == "," or letter == ")":
                letter = ""
            new = Tk()
            label1 = Label(new, text="Profitable customer count:\n\n |CUSTOMERS COUNT : {}\n".format(letter), font="30")
            label1.grid(row=0, column=0)
            canvas = Canvas(new, width=500, height=30)
            canvas.grid(columnspan = 1, rowspan = 6)


# Creating view for function.
def overTwentyTaxView():
    cursor.execute("CREATE VIEW CustomersWithTaxOver20Percent as "\
                   "select concat(customers.first_name, ' ',  "\
                   "customers.last_name) as CustomersWithTaxOver20Percent, customers.ID as ID, "\
                   "avg(heldMonths) as AverageHoldPeriodPerWallet "\
                   "from customers "\
                   "join transactions on customers.walletAdress = transactions.transactionWallet "\
                   "where heldMonths > 12 "\
                   "GROUP BY customers.ID "\
                   "ORDER BY customers.ID desc;")










