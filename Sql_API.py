from flask import Flask, jsonify
import pyodbc
import requests
import base64
import logging
from typing import Optional

app = Flask(__name__)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Daftar database dalam 1 server
databases = ["ED-02", "ED-03", "ED-04"]  # ganti sesuai DB mu

# Connection configuration
DB_CONFIG = {
    "DRIVER": "{ODBC Driver 17 for SQL Server}",
    "SERVER": "localhost",
    "Trusted_Connection": "yes"
}

# 🔗 URL GAS Web App akan diminta saat startup
GAS_URL: Optional[str] = None

def get_gas_url():
    global GAS_URL
    if GAS_URL:
        return GAS_URL

    # Try to read from a local 'url' file next to this script
    try:
        with open('url', 'r', encoding='utf-8') as f:
            val = f.read().strip()
            if val:
                GAS_URL = val
                logger.info('GAS URL loaded from file')
                return GAS_URL
    except FileNotFoundError:
        pass
    except Exception as e:
        logger.warning(f'Could not read url file: {e}')

    # Fallback: prompt and save to file
    val = input("Masukkan URL Google Apps Script (GAS): ").strip()
    if val:
        try:
            with open('url', 'w', encoding='utf-8') as f:
                f.write(val)
            logger.info('GAS URL saved to file')
        except Exception as e:
            logger.warning(f'Failed to save url file: {e}')
        GAS_URL = val
    return GAS_URL

def get_db_connection(database="ED-02"):
    """Create and return a database connection"""
    conn_str = ";".join([
        f"DRIVER={DB_CONFIG['DRIVER']}",
        f"SERVER={DB_CONFIG['SERVER']}",
        f"DATABASE={database}",
        f"Trusted_Connection={DB_CONFIG['Trusted_Connection']}"
    ])
    try:
        conn = pyodbc.connect(conn_str)
        return conn
    except pyodbc.Error as e:
        logger.error(f"Database connection error: {str(e)}")
        raise

def refresh_databases():
    """Refresh the global `databases` list by querying local SQL Server for online user databases."""
    global databases
    conn = None
    try:
        # Connect to master to query sys.databases
        conn = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=master;Trusted_Connection=yes;")
        cursor = conn.cursor()
        # Select user databases that are online (state_desc = 'ONLINE') and exclude system DBs
        cursor.execute("SELECT name FROM sys.databases WHERE database_id > 4 AND state_desc = 'ONLINE'")
        rows = cursor.fetchall()
        refreshed = [r[0] for r in rows]
        if refreshed:
            databases = refreshed
            logger.info(f"Refreshed databases: {databases}")
        else:
            logger.info("No user databases found or none online; keeping existing list")
    except Exception as e:
        logger.error(f"Failed to refresh databases: {e}")
    finally:
        if conn:
            conn.close()

@app.route('/tesdata')
def getdata():
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT TOP 100 * FROM dbo._modul")  # ganti nama tabel kamu

        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()

        # kolom yang kamu mau
        keep = {"msisdn", "temp"}

        results = []
        for row in rows:
            row_dict = {}
            for i, value in enumerate(row):
                col = columns[i]
                if col not in keep:
                    continue  # skip kolom lain
                if isinstance(value, bytes):
                    value = base64.b64encode(value).decode('utf-8')
                row_dict[col] = value
            results.append(row_dict)

        logger.info(f"Successfully retrieved {len(results)} records")
        return jsonify(results)
    except Exception as e:
        logger.error(f"Error in /getdata: {str(e)}")
        return jsonify({"error": str(e)}), 500
    finally:
        if conn:
            conn.close()

def select_database():
    # Refresh available databases dynamically
    refresh_databases()
    print("\nPilih Database:")
    for idx, db in enumerate(databases, 1):
        print(f"{idx}. {db}")
    while True:
        try:
            choice = input("\nMasukkan nomor database (1-" + str(len(databases)) + "): ")
            idx = int(choice) - 1
            if 0 <= idx < len(databases):
                return databases[idx]
            print("Pilihan tidak valid!")
        except ValueError:
            print("Masukkan nomor yang valid!")

@app.route('/SatuDatabase')
def sync_to_gsheet():
    conn = None
    try:
        # 1️⃣ Pilih dan Koneksi SQL Server
        selected_db = select_database()
        logger.info(f"Selected database: {selected_db}")
        
        conn = get_db_connection(database=selected_db)
        cursor = conn.cursor()
        cursor.execute("SELECT ALL * FROM dbo._modul")  # ganti tabelmu

        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()

        # 2️⃣ Filter kolom yang diperlukan
        keep = {"msisdn", "temp", "active"}
        data = []
        for row in rows:
            row_dict = {"DB": selected_db}  # Add database name as first column
            for i, value in enumerate(row):
                col = columns[i]
                if col not in keep:
                    continue
                if isinstance(value, bytes):
                    value = base64.b64encode(value).decode('utf-8')
                row_dict[col] = value
            data.append(row_dict)

        if not data:
            logger.info("No data found to sync")
            return jsonify({"message": "No data to push"})

        # 3️⃣ Kirim data ke GAS via POST
        gas_url = get_gas_url()  # Get the GAS URL
        logger.info(f"Sending {len(data)} records to GAS")
        res = requests.post(gas_url, json=data)
        res.raise_for_status()  # Raise an exception for bad status codes

        # 4️⃣ Kembalikan hasil response
        logger.info(f"Successfully synced {len(data)} records to GAS")
        return jsonify({
            "rows_pushed": len(data),
            "gas_response": res.text
        })

    except requests.exceptions.RequestException as e:
        logger.error(f"Error sending data to GAS: {str(e)}")
        return jsonify({"error": f"GAS sync error: {str(e)}"}), 500
    except Exception as e:
        logger.error(f"Error in /sync: {str(e)}")
        return jsonify({"error": str(e)}), 500
    finally:
        if conn:
            conn.close()

@app.route('/BanyakDatabase')
def sync_batch_to_gsheet():
    all_data = []
    errors = []
    
    try:
        # Refresh list of databases before processing so we use the current, online DBs
        refresh_databases()
        gas_url = get_gas_url()  # Get the GAS URL before starting batch sync
        
        for db_name in databases:
            conn = None
            try:
                logger.info(f"Processing database: {db_name}")
                conn = get_db_connection(database=db_name)
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM dbo._modul")
                
                columns = [col[0] for col in cursor.description]
                rows = cursor.fetchall()
                
                # Filter kolom
                keep = {"msisdn", "temp", "active"}
                db_data = []
                
                for row in rows:
                    row_dict = {"DB": db_name}  # Add database name as first column
                    for i, value in enumerate(row):
                        col = columns[i]
                        if col not in keep:
                            continue
                        if isinstance(value, bytes):
                            value = base64.b64encode(value).decode('utf-8')
                        row_dict[col] = value
                    db_data.append(row_dict)
                
                if db_data:
                    all_data.extend(db_data)
                    logger.info(f"Added {len(db_data)} records from {db_name}")
                
            except Exception as e:
                error_msg = f"Error processing {db_name}: {str(e)}"
                logger.error(error_msg)
                errors.append(error_msg)
            finally:
                if conn:
                    conn.close()
        
        if not all_data:
            return jsonify({"message": "No data to push", "errors": errors})
        
        # Kirim semua data ke GAS
        logger.info(f"Sending {len(all_data)} total records to GAS")
        res = requests.post(gas_url, json=all_data)
        res.raise_for_status()
        
        return jsonify({
            "total_rows_pushed": len(all_data),
            "databases_processed": len(databases),
            "errors": errors,
            "gas_response": res.text
        })
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error sending data to GAS: {str(e)}")
        return jsonify({
            "error": f"GAS sync error: {str(e)}",
            "partial_errors": errors
        }), 500
    except Exception as e:
        logger.error(f"Error in batch sync: {str(e)}")
        return jsonify({
            "error": str(e),
            "partial_errors": errors
        }), 500

if __name__ == '__main__':
    # Get GAS URL at startup
    get_gas_url()
    # Run the Flask app
    app.run(debug=True, port=5000)