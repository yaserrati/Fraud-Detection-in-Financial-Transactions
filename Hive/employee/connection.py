from pyhive import hive
import requests as req

def create_table(cursor):
    table_creation_query = """
    CREATE TABLE IF NOT EXISTS testdb.customers (
        account_history STRING,
        avg_transaction_value DOUBLE,
        customer_id STRING,
        age INT,
        location STRING
    )
    """
    cursor.execute(table_creation_query)

def insert_data(cursor, data):
    account_history_string = ",".join(data["account_history"])
    insert_query = '''
    INSERT INTO testdb.customers
    VALUES ('{a}',
    '{b}',
    '{c}',
    {d},
    '{e}')
    '''.format(
        a=account_history_string,
        b=data['behavioral_patterns']['avg_transaction_value'],
        c=data['customer_id'],
        d=data['demographics']['age'],
        e=data['demographics']['location']
    )
    cursor.execute(insert_query)

def main():
    # Get data from the API
    transactions = req.get("http://127.0.0.1:5000/api/customers/")

    if transactions.status_code == 200:
        result = transactions.json()

        # Establish a connection
        connection = hive.connect(host='localhost', database='testdb')
        cursor = connection.cursor()

        try:
            # Create the table
            create_table(cursor)

            # Insert data into the table
            for data in result:
                insert_data(cursor, data)

                # Commit the transaction
                connection.commit()

                # Fetch and print the inserted data
                cursor.execute('SELECT * FROM testdb.customers')
                print(cursor.fetchall())

            print("Data insertion completed.")
        except Exception as e:
            print(f"Error: {str(e)}")
        finally:
            # Close the cursor and connection
            cursor.close()
            connection.close()
    else:
        print("Failed to fetch data from the API.")

if __name__ == "__main__":
    main()
