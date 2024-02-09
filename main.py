import subprocess
import sys
import requests  # Import requests module

def select_primary_table():
    while True:
        print("Select a primary table:")
        print("1. User")
        print("2. Faction")
        print("3. Company")
        print("4. Property")
        print("5. Market")
        print("6. Torn")
        print("7. Key")
        choice = input("Enter the number of your choice: ")
        if choice == '1':
            return 'user'
        elif choice == '2':
            return 'faction'
        elif choice == '3':
            return 'company'
        elif choice == '4':
            return 'property'
        elif choice == '5':
            return 'market'
        elif choice == '6':
            return 'torn'
        elif choice == '7':
            return 'key'
        else:
            print("Invalid choice. Please select a valid primary table.")

def select_subtable(primary_table):
    while True:
        print(f"Select a subtable for {primary_table}:")
        if primary_table == 'user':
            print("1. Basic")
            # Add more subtables for the 'user' table as needed
        elif primary_table == 'faction':
            print("1. Attacks")
            # Add more subtables for the 'faction' table as needed
        # Add more subtables for other primary tables as needed

        print("0. Go back to previous question")
        choice = input("Enter the number of your choice: ")
        if primary_table == 'user':
            if choice == '1':
                return 'basic'
            # Add more subtable choices for the 'user' table as needed
        elif primary_table == 'faction':
            if choice == '1':
                return 'attacks'
            # Add more subtable choices for the 'faction' table as needed
        # Add subtable choices for other primary tables as needed
        elif choice == '0':
            return None
        else:
            print("Invalid choice. Please select a valid subtable.")

def execute_subtable_script(primary_table, subtable):
    if primary_table == 'user':
        if subtable == 'basic':
            print("Executing user_basic_main.py...")
            subprocess.call(["python", "user_basic_main.py"])
            sys.exit()
        # Execute other subtable scripts for the 'user' table as needed
    elif primary_table == 'faction':
        if subtable == 'attacks':
            print("Executing faction_attacks_main.py...")
            subprocess.call(["python", "faction_attacks_main.py"])
            sys.exit()
        # Execute other subtable scripts for the 'faction' table as needed
    # Execute subtable scripts for other primary tables as needed

if __name__ == "__main__":
    while True:
        primary_table = select_primary_table()
        subtable = select_subtable(primary_table)
        if subtable:
            execute_subtable_script(primary_table, subtable)
        else:
            print("Exiting script.")
            break
