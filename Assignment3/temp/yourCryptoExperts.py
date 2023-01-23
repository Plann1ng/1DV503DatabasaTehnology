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
