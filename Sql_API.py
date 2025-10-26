from flask import Flask, jsonify, request, redirect, url_for, render_template, flash
import pyodbc
import requests
import base64
import logging
from typing import Optional

app = Flask(__name__)
app.secret_key = "change-me-to-a-random-secret"

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Daftar database dalam 1 server
databases = ["ED-02", "ED-03", "ED-04"]  # ganti sesuai DB mu

# Connection configuration
DB_CONFIG = {
    "DRIVER": "{ODBC Driver 17 for SQL Server}",
    "SERVER": "localhost",  # Will be updated from settings
    "Trusted_Connection": "yes"
}

# 🔗 URL GAS Web App dan Server settings akan diminta saat startup
GAS_URL: Optional[str] = None
SERVER_TYPE: Optional[str] = None  # 'EXPRESS' or 'DEFAULT'

def get_server_type():
    """Get the server type from settings file"""
    global SERVER_TYPE
    if SERVER_TYPE:
        return SERVER_TYPE
    try:
        with open('server_type', 'r', encoding='utf-8') as f:
            val = f.read().strip().upper()
            if val in ['EXPRESS', 'DEFAULT']:
                SERVER_TYPE = val
                logger.info(f'Server type loaded from file: {val}')
                return SERVER_TYPE
    except FileNotFoundError:
        logger.info('No server_type file found')
    except Exception as e:
        logger.warning(f'Could not read server_type file: {e}')
    return None

def save_server_type(server_type: str):
    """Save server type to settings file"""
    global SERVER_TYPE
    server_type = server_type.upper()
    if server_type not in ['EXPRESS', 'DEFAULT']:
        raise ValueError("Server type must be 'EXPRESS' or 'DEFAULT'")
    
    try:
        with open('server_type', 'w', encoding='utf-8') as f:
            f.write(server_type)
        SERVER_TYPE = server_type
        # Update DB_CONFIG with new server
        DB_CONFIG["SERVER"] = "localhost\\SQLEXPRESS" if server_type == "EXPRESS" else "localhost"
        logger.info(f'Server type saved to file: {server_type}')
    except Exception as e:
        logger.error(f'Failed to save server type: {e}')
        raise

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
        # No file yet; return None and let the UI accept one
        logger.info('No url file found; GAS URL not set')
        return None
    except Exception as e:
        logger.warning(f'Could not read url file: {e}')
        return None

    return None

def get_db_connection(database="ED-02"):
    """Create and return a database connection"""
    # Ensure server type is loaded
    if not get_server_type():
        raise ValueError("Server type not configured. Please set it in the web UI first.")
    
    conn_str = ";".join([
        f"DRIVER={DB_CONFIG['DRIVER']}",
        f"SERVER={DB_CONFIG['SERVER']}",  # This will be either localhost or localhost\SQLEXPRESS
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
        # Ensure server type is configured
        if not get_server_type():
            raise ValueError("Server type not configured. Please set it in the web UI first.")
            
        # Use the configured server
        conn_str = f"DRIVER={DB_CONFIG['DRIVER']};SERVER={DB_CONFIG['SERVER']};DATABASE=master;Trusted_Connection=yes;"
        conn = pyodbc.connect(conn_str)
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


def build_data_for_db(db_name: str, keep_columns=None):
    """Return list of dict rows for a given database."""
    if keep_columns is None:
        keep_columns = {"msisdn", "temp", "active"}

    conn = None
    rows_out = []
    try:
        conn = get_db_connection(database=db_name)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM dbo._modul")

        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()

        for row in rows:
            row_dict = {"DB": db_name}
            for i, value in enumerate(row):
                col = columns[i]
                if col not in keep_columns:
                    continue
                if isinstance(value, bytes):
                    value = base64.b64encode(value).decode('utf-8')
                row_dict[col] = value
            rows_out.append(row_dict)
    finally:
        if conn:
            conn.close()
    return rows_out


def send_to_gas(data):
    gas_url = get_gas_url()
    if not gas_url:
        raise ValueError("GAS URL is not set. Set it via the web UI at /set_gas_url")
    res = requests.post(gas_url, json=data)
    res.raise_for_status()
    return res

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
    # Legacy console selection removed. Use web UI endpoints instead.
    raise RuntimeError("select_database() should not be used in the web UI mode")

@app.route('/SatuDatabase')
def sync_to_gsheet():
    # Keep for compatibility but require query param ?db=<name>
    db = request.args.get('db')
    if not db:
        return jsonify({"error": "Please provide ?db=<database_name> or use the web UI."}), 400
    try:
        data = build_data_for_db(db)
        if not data:
            return jsonify({"message": "No data to push"})
        res = send_to_gas(data)
        logger.info(f"Successfully synced {len(data)} records to GAS for {db}")
        return jsonify({"rows_pushed": len(data), "gas_response": res.text})
    except requests.exceptions.RequestException as e:
        logger.error(f"Error sending data to GAS: {str(e)}")
        return jsonify({"error": f"GAS sync error: {str(e)}"}), 500
    except Exception as e:
        logger.error(f"Error in /SatuDatabase: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/BanyakDatabase')
def sync_batch_to_gsheet():
    # For compatibility: run batch if called directly. Prefer using / (UI).
    all_data = []
    errors = []
    try:
        refresh_databases()
        for db_name in databases:
            try:
                logger.info(f"Processing database: {db_name}")
                db_data = build_data_for_db(db_name)
                if db_data:
                    all_data.extend(db_data)
                    logger.info(f"Added {len(db_data)} records from {db_name}")
            except Exception as e:
                error_msg = f"Error processing {db_name}: {str(e)}"
                logger.error(error_msg)
                errors.append(error_msg)

        if not all_data:
            return jsonify({"message": "No data to push", "errors": errors})

        res = send_to_gas(all_data)
        return jsonify({
            "total_rows_pushed": len(all_data),
            "databases_processed": len(databases),
            "errors": errors,
            "gas_response": res.text
        })
    except requests.exceptions.RequestException as e:
        logger.error(f"Error sending data to GAS: {str(e)}")
        return jsonify({"error": f"GAS sync error: {str(e)}", "partial_errors": errors}), 500
    except Exception as e:
        logger.error(f"Error in batch sync: {str(e)}")
        return jsonify({"error": str(e), "partial_errors": errors}), 500


### Web UI endpoints (move prompts to web)


@app.route('/')
def index():
    # show dashboard
    try:
        refresh_databases()
    except ValueError:
        # If server type not configured, just continue to show the UI
        pass
    return render_template('index.html', 
                         gas_url=get_gas_url(), 
                         server_type=get_server_type(),
                         databases=databases, 
                         message=None)


@app.route('/set_gas_url', methods=['POST'])
def set_gas_url():
    global GAS_URL
    val = request.form.get('gas_url', '').strip()
    if not val:
        flash('Please provide a non-empty URL')
        return redirect(url_for('index'))
    try:
        with open('url', 'w', encoding='utf-8') as f:
            f.write(val)
        GAS_URL = val
        logger.info('GAS URL saved to file via web UI')
        return redirect(url_for('index'))
    except Exception as e:
        logger.error(f'Failed to save GAS URL: {e}')
        return jsonify({'error': str(e)}), 500

@app.route('/set_server_type', methods=['POST'])
def set_server_type_route():
    val = request.form.get('server_type', '').strip().upper()
    if not val or val not in ['EXPRESS', 'DEFAULT']:
        flash('Please select a valid server type')
        return redirect(url_for('index'))
    try:
        save_server_type(val)
        # After changing server, refresh the database list
        try:
            refresh_databases()
        except Exception as e:
            flash(f'Server type saved but failed to connect: {str(e)}')
        return redirect(url_for('index'))
    except Exception as e:
        logger.error(f'Failed to save server type: {e}')
        return jsonify({'error': str(e)}), 500


@app.route('/refresh_databases', methods=['POST'])
def refresh_databases_route():
    try:
        refresh_databases()
        return redirect(url_for('index'))
    except Exception as e:
        logger.error(f'Error refreshing databases: {e}')
        return jsonify({'error': str(e)}), 500


@app.route('/sync_one', methods=['POST'])
def sync_one():
    db = request.form.get('database')
    if not db:
        return jsonify({'error': 'No database selected'}), 400
    try:
        data = build_data_for_db(db)
        if not data:
            return render_template('index.html', gas_url=get_gas_url(), databases=databases, message='No data to push')
        res = send_to_gas(data)
        msg = f"Pushed {len(data)} rows from {db}. GAS response: {res.text}"
        return render_template('index.html', gas_url=get_gas_url(), databases=databases, message=msg)
    except Exception as e:
        logger.error(f'Error in sync_one: {e}')
        return render_template('index.html', gas_url=get_gas_url(), databases=databases, message=str(e)), 500


@app.route('/sync_batch', methods=['POST'])
def sync_batch():
    try:
        refresh_databases()
        all_data = []
        errors = []
        for db_name in databases:
            try:
                db_rows = build_data_for_db(db_name)
                if db_rows:
                    all_data.extend(db_rows)
            except Exception as e:
                errors.append(f"{db_name}: {e}")

        if not all_data:
            return render_template('index.html', gas_url=get_gas_url(), databases=databases, message=f'No data to push. Errors: {errors}')

        res = send_to_gas(all_data)
        msg = f"Pushed {len(all_data)} rows from {len(databases)} DBs. GAS response: {res.text}. Errors: {errors}"
        return render_template('index.html', gas_url=get_gas_url(), databases=databases, message=msg)
    except Exception as e:
        logger.error(f'Error in sync_batch (UI): {e}')
    return render_template('index.html', gas_url=get_gas_url(), databases=databases, message=str(e)), 500

if __name__ == '__main__':
    # Get GAS URL at startup
    get_gas_url()
    # Run the Flask app
    app.run(debug=True, port=5000)