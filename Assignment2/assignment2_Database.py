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

# Special print statement for second query
# NOTE tried to install prettytables which could've made this table much more prettier, however
# failed miserable and I'm sorry for the ugly print, if I wasn't running out of time I could've fixed that.
def choiceTwoPrint(cursor):
    print("\nFetched results: ")
    print("+{}+".format("-"*130))
    print("|{:<11}|{:<1} |{:<1} |{:<1} |{:<9} |{:<11} |{:<26} |{:<1} |{:<1}|".format("name", "rotation_period", "orbital_period", "diameter", "climate", "gravity"," terrain", "surface_water", "population"))
    for name, rotation_period, orbital_period, diameter, climate, gravity, terrain, surface_water, population in cursor:
      print("+{}+".format("-"*130))  
      print("|{:<10} |{:<15} |{:<14} |{:<9}|{:<7} |{:<11} |{:<15} |{:<13} |{:<10}|".format(name, rotation_period, orbital_period, diameter, climate, gravity, terrain, surface_water, population))
      print("+{}+".format("-"*130))

# Special print statement for third query
def choiceThreePrint(cursor):
    print("\nFetched results: ")
    print("+{}+".format("-"*66))
    for name, average_height in cursor:
        print("|{}|{:<15}|".format(name, average_height))
        print("+{}+".format("-"*66))

# Special print statement for fifth query
def choiceFivePrint(cursor):
    print("\nFetched results: ")
    print("+{}+".format("-"*70))
    print("| {:<50} | {}|".format("clasification", "average_lifespan"))
    print("+{}+".format("-"*70))
    # turned average lifespan to string, because otherwise formatting was raising error.
    for classification, average_lifespan in cursor:
      average_lifespan = str(average_lifespan)
      if isinstance(classification, str):
          # pass the mammals instance.
          if "als" in classification:
              continue
          else:
           print("| {} | {:<15} |".format(classification, average_lifespan))
           print("+{}+".format("-"*70))

# User choice one
# Not raising any errors since the function doesn't take any user input.
def choiceOne():
    cursor.execute("SELECT name FROM {}".format("planets"))
    clearPrint(cursor)

# User choice two
def choiceTwo():
    nam =  '"' + str(input("Input planet name:\n")) + '"'
    # Raise error if the planet name is shorter than 3, encapsulation focused statement.
    if len(nam) <= 3:
        raise ValueError("'\x1b[6;30;42m Name should not be empty please tap [1] and choose planet name:'\x1b[0m'")
    # Select everything from planets where name of the planet is equal to user input.
    else:
        cursor.execute('SELECT * FROM {} WHERE Name={}'.format("planets",nam))
        choiceTwoPrint(cursor)


# User choice three
def choiceThree():
    num = int(input("Provide average height for species: "))
    # Raise error for invalid values.
    if num < 0:
        raise ValueError("'\x1b[6;30;42m lenght must be positive value!'\x1b[0m'")
    # Select name and height from species with height bigger than the input.
    else:
        cursor.execute('SELECT name, average_height FROM {} WHERE {} > {}'.format("species", "average_height", num ))
        choiceThreePrint(cursor)

# User choice four
def choiceFour():
    # By whitespace, because otherwise it is becoming more complicated to seperate them.
    nam = str(input("List species' name (if you want to search for multiple species at once input names followed by whitespace: "))
    # validating user input
    if len(nam) <= 1:
        raise ValueError("'\x1b[6;30;42m Name should not be empty please tap [1] and choose planet name:'\x1b[0m'")
    # NOTE made it so that user can input one or multiple names at once and get corresponding results which is more flexible than calling the query multiple times to check
    # if user inputs one name only.
    if " " not in nam:
        cursor.execute("SELECT species.`name`, planets.`climate` FROM planets JOIN species on planets.`name` = species.`homeworld` WHERE `species`.name = '{}'".format(nam))
        print("\n+{}+".format("-"*66))
        for name, climate in cursor:
            name = str(name)
            climate = str(climate)
            chars = re.escape(string.punctuation)
            print("|{}|{:<15}|".format(re.sub(r'['+chars+']', '',name), (re.sub(r'['+chars+']', '',climate))))
        print("+{}+".format("-"*66))
    else:
        # Turn the input to list
        nam = nam.split()
        query = ''
        # Special condition avoiding "OR" statement within query statement
        if len(nam) == 1:
            query = "species.`name` = '{}'".format(nam)
        # Multiple names
        elif len(nam) > 1:
            # First part of the query takes the first name without "OR" function
            query += "species.`name` = '{}' ".format(nam[0])
            # Taking rest of the names with included "OR"
            for i in range(len(nam) -1):
                query += " OR species.`name` = '{}' ".format(nam[i + 1])
        # Execute query joining species' homeworld to planets' name and ordering them and simply setting the limit to one to get the most common climate.
        # NOTE it was weird that it wasn't even necesarry to even state foreign key while creating the tables.
        cursor.execute('SELECT species.`name`, planets.`climate` FROM planets JOIN species on planets.`name` = species.`homeworld` WHERE {} ORDER BY climate DESC'.format(query))
        # Getting rid of the special characters and returning the input.
        print("\n+{}+".format("-"*9))
        for i in cursor:
            i = str(i)
            chars = re.escape(string.punctuation)
            print("|{}|".format(re.sub(r'['+chars+']', '',i)))
        print("+{}+".format("-"*9))

# User choice five
def choiceFive():
    # Selecting all the collumns that were requested
    cursor.execute('SELECT {}, avg({}) FROM {} group by classification'.format("classification","average_lifespan", "species"))
    choiceFivePrint(cursor)

# Main menu where the user will iterate.
def mainMenu():
    print("+{}+".format("-"*60))
    print("|\n| 1. List all the planets:")
    print("| 2. Search for planer details:")
    print("| 3. Search for species with height higher than given number:")
    print("| 4. What is the most likely climate of the given species:")
    print("| 5. what is the average lifespan per species clasification:")
    print("| Q: Quit\n|")
    print("+{}+".format("-"*60))
    userChoice = input()

    # Taking the inputs as string, since we have mix of letter and numbers.
    if userChoice == "1":
        choiceOne()
        # Returning True statements to keep the while loop running.
        return True
    elif userChoice == "2":
        choiceTwo()
        return True 

    elif userChoice == "3":
        choiceThree()
        return True
    elif userChoice == "4":
        choiceFour()
        return True
    elif userChoice == "5":
        choiceFive()
        return True
    # Returning false to state that the user quits.
    elif userChoice == "Q" or userChoice == "q":
        # Disconnect from the database
        cursor.close()
        cnx.close()
        print("\n\x1b[6;30;42m Disconnecting... \x1b[0m\n")
        print("\x1b[6;30;42m Bye! \x1b[0m")
        return False
    else:
    # Validation for the inputs.
        print("\x1b[6;30;42m Follow the instructions! \x1b[0m")
        return True

# Variable to start the loop
valid = True
while valid:
    # Call the main method until it is closed.
    mainMenu()
    print("\n")
    # Press any key to return to main menu
    wait()
    # break the loop.
    if mainMenu() is False:
        valid is False
        break







        

        