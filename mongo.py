'''--------------------------------------
                Imports
--------------------------------------'''

import pymongo # MongoDB Python interface
import sys # Allows for exiting program
import datetime # Required for modifying/searching for timestamps
import os # Required for running search command externally
from time import sleep # Pauses program for user flow
import bson # Converting strings to objectID
import csv # Exports and imports CSV

'''--------------------------------------
                Globals
--------------------------------------'''
# Allows all functions to use user's db selections without additional arguments
global user_db_selection
global localOrRemote 
global db

'''--------------------------------------
               Functions
--------------------------------------'''
# Terminal clear function
def clear(): 
    # for windows 
    if os.name == 'nt': 
        _ = os.system('cls') 
  
    # for mac and linux
    else: 
        _ = os.system('clear') 

# Connects to mongoDB server through SSH tunnel.
def connect(host,user,pw):
    server = SSHTunnelForwarder(
        host,
        ssh_username=user,
        ssh_password=pw,
        remote_bind_address=('127.0.0.1', 27017)
    )
    server.start()
    client = pymongo.MongoClient('127.0.0.1', server.local_bind_port)
    return select_database(client)

# Displays and selects MongoDB database
def select_database(client):
    valid = False
    while not valid:
        clear()
        print("\nAvailable databases: ")
        db_list = []
        db_counter = 1
        # Access database names thru client
        for dba in client.list_databases():
            print("(" + str(db_counter) + ") " + dba['name'])
            db_counter += 1
            db_list.append(dba['name'])
        global user_db_selection
        user_db_selection = raw_input("-\nChoose a database to explore: ")
        if not user_db_selection.isdigit():
            print("Invalid input! Try again.\n")
        else:
            user_db_selection = int(user_db_selection)
            valid = True
    user_db_selection = db_list[user_db_selection - 1]
    print("Selected '" + user_db_selection + "'\n")
    return client[user_db_selection]

# Displays and selects MongoDB collection
def select_collection(all_collections):
    valid = False
    while not valid:
        for i in range(len(all_collections)):
            print("(" + str(i + 1) + ") "  +all_collections[i])
        global selection 
        selection = raw_input("-\nWhich collection would you like to select? Type 'e' or 'exit' to exit the program.")
        if selection == "e" or selection == "exit":
            return
        elif selection.isdigit():
            selection = int(selection)
            valid = True
        else:
            print("Invalid input, try again!\n")
    print("Selected '" + all_collections[selection - 1] + "'\n")
    print("-------------------------------------------")

    #Storing name of collection for export function
    global collection_selection
    collection_selection = all_collections[selection - 1]

    filter_results(db[all_collections[selection - 1]])

# Prints results of search
def print_results(selected_collection,search_query):
    print("\n\nSearching collections...\nGenerating export command...")
    sleep(1)
    clear()

    
    #displays a list of search queries 
    query_output = ""
    counter = 0
    for key in search_query:
        counter += 1
        if len(search_query) != 0 and counter != len(search_query):
            query_output += ", "
        query_output += key
    if len(query_output) == 0:
        query_output = "None"
    print("Filters: " + query_output)
    
    #Here, we're trying to set the same variables using different methods based on the type of data structure
    # For list entries
    if type(selected_collection) == list:
        length = len(selected_collection)
        print("\n")
        print(selected_collection)
    # For dictionary entries
    elif type(selected_collection) == dict:
        length = len(selected_collection)
    # For other entry types (usually pymongo.collection.Collection)
    else: 
        documents = selected_collection.find(search_query)
        length = documents.count()
    counter = 0
    #Problem: what if database consists of many "levels" of nested data structures?
    '''Solution: Create a generic way to traverse and display any type of data structure (in this function)
     x Create list/counter to store submenus in for easy selection 
     x Allow user to select a number from our counter, which will recursively call our display function on the contents of that index in submenu list
     - Refactor display function to work with both lists and dicts (currently only dicts)
     - Allow user to exit out of recursive level, returning to previous list display.

     Note: We could try and turn everything into a dictionary. If we do so, we need to convert only the 5 entries we're displaying before printing them out.
     This solution is not fully implemented, as the collections in our debug db do not need to be explored.
     '''

    #Counter/List to store all the nested structures. They'll be displayed as "Type (subMenuSelectionCounter)", and the user can use this counter to select a structure to open. 
    subMenuSelectionCounter = 0
    subMenuSelectionList = []

    #Main print loop- works with pymongo.collection.Collections and most likely dictionaries.
    while True:
        print("\nDisplaying documents " + str(counter) + "-" + str(counter + 5) + " of " + str(length) + " total.")
        #Adjust length of range to show more/less per page
        for i in range(5):
            counter += 1
            print("-------------------------------------------")
            #Break out of loop once we display more than the range.
            if counter > length:
                break
            #Entry display
            for key in documents[counter]:
                #Check if list or dict
                if type(documents[counter][key]) == list :
                    #Add to submenu selection list and add 1 to counter (counter = index - 1)
                    subMenuSelectionList.append(documents[counter][key])
                    subMenuSelectionCounter += 1
                    print('{:<20}{:<20}'.format(str(key),"List (" + str(subMenuSelectionCounter) + ")"))
                elif type(documents[counter][key]) == dict:
                    subMenuSelectionList.append(documents[counter][key])
                    subMenuSelectionCounter += 1
                    print('{:<20}{:<20}'.format(str(key),"Dict (" + str(subMenuSelectionCounter) + ")"))
                #If not either, print as normal
                else:
                    print('{:<20}{:<20}'.format(str(key),str((documents[counter][key]))))
        #Display menu
        valid = False
        while not valid:
            displayPrompt = raw_input("\n'next'/'n': next page. 'exit'/'e': leave search results. 'export'/'ex': export search to file.").lower()
            print("===========================================")
            if displayPrompt == "next" or displayPrompt == "n":
                valid = True
                continue
            elif displayPrompt == "exit" or displayPrompt == "e":
                valid = True
                #Call select collection function. Requires db variable to be globally defined.
                select_collection(db.collection_names())
                return
            elif displayPrompt == "exit" or displayPrompt == "ex":
                #Create export command for the current search
                generate_export_command(selected_collection,search_query)
                return
            else:
                #Recursive call to display contents of selected index within subMenuSelectionList
                print("Invalid input, try again.")
                #print_results(subMenuSelectionList[int(displayPrompt)],[])

# Allows user to add/define search queries to be displayed
def filter_results(temp):       
    #Some notes: remote debug server uses both bson.objectID and standard objectID in entries, which makes it tricky to apply filters
    display_selection = raw_input("Choose a method to display collection:\n\t(1) Search\n\t(2) List\n").upper()
    if display_selection == "1":
        currentlyFiltering = True
        query = {}
        while currentlyFiltering == True:
            clear()
            print("-------------------------------------------")
            filters=""
            for key in query:
                filters += str(key)
                filters += ", "
            if filters == "":
                print("Current filters: None")
            else:
                print("Current filters: " + filters)
            print("\n")


            documents = temp.find({})
            #Grab first entry in collection, back up all keys (and corresponding value types) into list, then display them as options.
            #Value types: str, int, ObjectID, Date
            searchTypes = {}
            for key in documents[0]:
                #Add key and sample data to dictionary. Key is for displaying the filter, sample type is to determine which datatype to convert inputted filter to.
                #Temporarily removing ID, and not displaying an option if it is of nonetype
                if str(key) != "_id" and documents[0][key] is not None:
                    searchTypes[str(key)] = documents[0][key]
            #Search prompt
            print("Available Filters:")
            counter = 0
            for key in searchTypes:
                counter += 1
                print("(" + str(counter) + ") " + key)
            search_selection = raw_input("-\nChoose one or multiple filters to search by, or 'done' to search with your selected filters:")
            if search_selection == "done":
                currentlyFiltering = False
            #If input is empty
            elif search_selection == "":
                print("Invalid input, try again.")
            #Check if input is a valid number
            elif search_selection in "1234567890":
                #A little janky- grabs the key from our inputted "index" of the dict
                counter = 0
                for key in searchTypes:
                    counter += 1
                    if counter == int(search_selection):
                        #Select the key from our searchTypes based off of inputted number
                        search_selection = key
                        break
                #Attempts to convert unicode, datetime, or bson objectID, using isInstance to determine type
                #If of time unicode
                if isinstance(searchTypes[search_selection],unicode):
                    #Assuming utf-8 encoding
                    query[search_selection] = unicode(raw_input("Enter Search String: "), "utf-8")

                #If of type datetime
                elif isinstance(searchTypes[search_selection],datetime.datetime):
                    global input_start_date
                    global input_end_date
                    input_start_date = raw_input("Enter the starting date (i.e 2010-01-20): ")
                    input_end_date = raw_input("Enter the ending date (i.e 2015-01-20): ")
                    #Creates a search query for all entries in between two datetime objects
                    start_date = input_start_date.split("-")
                    end_date = input_end_date.split("-")
                    start = datetime.datetime(int(start_date[0]), int(start_date[1]), int(start_date[2]))
                    end = datetime.datetime(int(end_date[0]), int(end_date[1]), int(end_date[2]))
                    query[search_selection] = {"$gte": start, "$lt": end }
                
                #If of types int and str
                elif isinstance(searchTypes[search_selection],int):
                    query[search_selection] = input("Enter Number: ")
                elif isinstance(searchTypes[search_selection],str):
                    query[search_selection] = input("Enter Search String: ")
                else:
                    print("This filter is of an unusual type, and cannot be searched with. Sorry!")
            else:
                print("Invalid input, try again.")
        print_results(temp,query)
        return
    else:
        print_results(temp,{})
        return

# Creates and runs Mongo command for exporting CSV of search query
def generate_export_command(selected_collection,search_query):
    #Select all keys from the first document in collection
    if search_query == "":
        print("Nothing to export!")
        return
    document = selected_collection.find(search_query)[0]
    keys = ""
    for key in document:
        keys += key
        keys += ","
    #Use slicing to remove the last comma
    keys = keys[:-1]


    export_query = "'{"

    #Stringifies queries so we can change our datetime into ISO format without importing any more libraries
    for key in search_query:
        if key == "timestamp":
            export_query += '"timestamp":{$gte: ISODate("' + input_start_date + 'T00:00:00Z"),$lt: ISODate("' + input_end_date + 'T00:00:00Z")}'
        else:
            export_query += '"' + key + '":' + str(search_query[key]) + ','
    export_query += "}'"
    clear()
    out = ""
    directory = raw_input("Output directory (leave blank for current directory): ")
    filename = raw_input("Output file name (Leave blank for 'output'): ")
    if filename == "":
        filename = "output.csv"
    elif not filename.endswith(".csv"):
        filename += ".csv"
    out = directory + filename
    

    #Output export command
    print("\n===========================================")
    if localOrRemote == "local" or localOrRemote == "l":
        os.system("mongoexport --db " + str(user_db_selection)+" --collection "+str(collection_selection)+" --type=csv --fields "+keys+" -q "+ export_query + " --out " + out)
    else:
        print("Copy (select and right-click) and paste this command into the shell to export your search to a .csv file:")
        print("-------------------------------------------")
        print("mongoexport --db " + str(user_db_selection)+" --collection "+str(collection_selection)+" --type=csv --fields "+keys+" -q "+ export_query + " --out " + out)
    print("===========================================\n")


'''--------------------------------------
               Main Code
--------------------------------------'''
# User has the option of connecting to a remote server (when program is locally run on a computer), or a local server (when program is run on the same server as the database)
localOrRemote = raw_input("Connect to local or remote server (l/r)? ")

if localOrRemote == "local" or localOrRemote == "l":
    client = pymongo.MongoClient('127.0.0.1', 27017)
    db = client['debug_data']
    user_db_selection = "debug_data"

elif localOrRemote == "remote" or localOrRemote == "r":
    from sshtunnel import SSHTunnelForwarder
    import getpass
    while True:
        try:
            MONGO_HOST = raw_input("Enter server hostname: ")
            MONGO_USER = raw_input("Enter Mongo username: ")
            MONGO_PASS = getpass.getpass("Enter Mongo password: ")
            #Uses SSH tunnel method ONLY for remote server
            db = connect(MONGO_HOST,MONGO_USER,MONGO_PASS)
            break
        except:
            clear()
            print("Wrong server, username, or password. Try again.")


select_collection(db.collection_names())

