from flask import Flask, jsonify, request, redirect, url_for, render_template, flash
import pyodbc
import requests
import base64
import logging
import webbrowser
import time
from typing import Optional

app = Flask(__name__)
app.secret_key = "change-me-to-a-random-secret"

# ---- Logging ----
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("sql2gas")

# ---- Defaults ----
databases = ["ED-02", "ED-03", "ED-04"]
CHUNK_SIZE_DEFAULT = 1000

DB_CONFIG = {
    "DRIVER": "{ODBC Driver 17 for SQL Server}",
    "SERVER": "localhost",
    "Trusted_Connection": "yes",
}

GAS_URL: Optional[str] = None
SERVER_TYPE: Optional[str] = None  # 'EXPRESS' | 'DEFAULT'


# ================= Settings helpers =================

def get_server_type():
    global SERVER_TYPE
    if SERVER_TYPE:
        return SERVER_TYPE
    try:
        with open("server_type", "r", encoding="utf-8") as f:
            val = f.read().strip().upper()
            if val in ["EXPRESS", "DEFAULT"]:
                SERVER_TYPE = val
                DB_CONFIG["SERVER"] = "localhost\\SQLEXPRESS" if val == "EXPRESS" else "localhost"
                logger.info(f"Server type loaded: {val} -> SERVER={DB_CONFIG['SERVER']}")
                return SERVER_TYPE
    except FileNotFoundError:
        logger.info("No server_type file found")
    except Exception as e:
        logger.warning(f"Could not read server_type: {e}")
    return None


def save_server_type(server_type: str):
    global SERVER_TYPE
    server_type = server_type.upper()
    if server_type not in ["EXPRESS", "DEFAULT"]:
        raise ValueError("Server type must be 'EXPRESS' or 'DEFAULT'")
    with open("server_type", "w", encoding="utf-8") as f:
        f.write(server_type)
    SERVER_TYPE = server_type
    DB_CONFIG["SERVER"] = "localhost\\SQLEXPRESS" if server_type == "EXPRESS" else "localhost"
    logger.info(f"Server type saved: {server_type} -> SERVER={DB_CONFIG['SERVER']}")


def get_gas_url():
    global GAS_URL
    if GAS_URL:
        return GAS_URL
    try:
        with open("url", "r", encoding="utf-8") as f:
            val = f.read().strip()
            if val:
                GAS_URL = val
                logger.info("GAS URL loaded from file")
                return GAS_URL
    except FileNotFoundError:
        logger.info("No url file found; GAS URL not set")
    except Exception as e:
        logger.warning(f"Could not read url file: {e}")
    return None


# ================= DB helpers =================

def get_db_connection(database="ED-02"):
    if not get_server_type():
        raise ValueError("Server type not configured. Set via UI first.")
    conn_str = ";".join([
        f"DRIVER={DB_CONFIG['DRIVER']}",
        f"SERVER={DB_CONFIG['SERVER']}",
        f"DATABASE={database}",
        f"Trusted_Connection={DB_CONFIG['Trusted_Connection']}",
    ])
    try:
        return pyodbc.connect(conn_str)
    except pyodbc.Error as e:
        logger.error(f"DB connect error: {e}")
        raise


def refresh_databases():
    global databases
    conn = None
    try:
        if not get_server_type():
            raise ValueError("Server type not configured.")
        conn_str = (
            f"DRIVER={DB_CONFIG['DRIVER']};SERVER={DB_CONFIG['SERVER']};"
            f"DATABASE=master;Trusted_Connection=yes;"
        )
        conn = pyodbc.connect(conn_str)
        cur = conn.cursor()
        cur.execute("SELECT name FROM sys.databases WHERE database_id > 4 AND state_desc = 'ONLINE'")
        rows = cur.fetchall()
        refreshed = [r[0] for r in rows]
        if refreshed:
            databases = refreshed
            logger.info(f"Refreshed databases: {databases}")
        else:
            logger.info("No user DBs found; keep existing list")
    except Exception as e:
        logger.error(f"Refresh DB list failed: {e}")
    finally:
        if conn:
            conn.close()


def clean_msisdn(val):
    """Remove spaces/dashes, leading '+' and '62' country code."""
    if val is None:
        return ""
    v = str(val).strip()
    if not v:
        return ""
    v = v.replace(" ", "").replace("-", "")
    if v.startswith("+"):
        v = v[1:]
    if v.startswith("62"):
        v = v[2:]
    return v


def build_data_for_db(db_name: str, keep_columns=None):
    if keep_columns is None:
        keep_columns = {"msisdn", "temp", "active"}

    rows_out = []
    conn = None
    try:
        conn = get_db_connection(database=db_name)
        cur = conn.cursor()
        cur.execute("SELECT * FROM dbo._modul")

        columns = [col[0] for col in cur.description]
        rows = cur.fetchall()

        for row in rows:
            row_dict = {"DB": db_name}
            for i, value in enumerate(row):
                col = columns[i]
                if col not in keep_columns:
                    continue
                if isinstance(value, bytes):
                    value = base64.b64encode(value).decode("utf-8")
                if col.lower() == "msisdn":
                    value = clean_msisdn(value)
                row_dict[col] = value
            rows_out.append(row_dict)
    finally:
        if conn:
            conn.close()
    return rows_out


# ================= GAS post (always chunked) =================

def iter_chunks(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def send_to_gas(data, timeout=60):
    gas_url = get_gas_url()
    if not gas_url:
        raise ValueError("GAS URL is not set. Set it at / via the form.")
    res = requests.post(
        gas_url,
        json=data,
        headers={"Content-Type": "application/json"},
        timeout=timeout,
    )
    res.raise_for_status()
    return res


def post_in_chunks(data, chunk_size=CHUNK_SIZE_DEFAULT, max_retries=2, retry_delay=1.0):
    """
    ALWAYS chunked delivery.
    Returns summary: {total_sent, total_chunks, results, errors, failed_chunks}
    """
    results, errors, failed_chunks = [], [], []
    total_sent, chunk_idx = 0, 0

    for chunk in iter_chunks(data, chunk_size):
        chunk_idx += 1
        attempt = 0
        while True:
            try:
                res = send_to_gas(chunk)
                total_sent += len(chunk)
                results.append({
                    "chunk": chunk_idx,
                    "rows": len(chunk),
                    "status": res.status_code,
                    "response": (res.text[:1000] if res.text else "")
                })
                break
            except requests.exceptions.RequestException as e:
                attempt += 1
                if attempt > max_retries:
                    msg = f"Chunk {chunk_idx} (rows={len(chunk)}) failed after {max_retries} retries: {e}"
                    logger.error(msg)
                    errors.append(msg)
                    failed_chunks.append({"chunk": chunk_idx, "rows": len(chunk), "error": str(e)})
                    break
                sleep_s = retry_delay * (2 ** (attempt - 1))
                logger.warning(f"Chunk {chunk_idx} failed (attempt {attempt}) -> retry in {sleep_s:.1f}s: {e}")
                time.sleep(sleep_s)

    return {
        "total_sent": total_sent,
        "total_chunks": chunk_idx,
        "results": results,
        "errors": errors,
        "failed_chunks": failed_chunks,
    }


# ================= API routes (UI juga pakai ini) =================

@app.route("/tesdata")
def getdata():
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT TOP 100 * FROM dbo._modul")
        columns = [c[0] for c in cur.description]
        rows = cur.fetchall()

        keep = {"msisdn", "temp"}
        out = []
        for row in rows:
            row_dict = {}
            for i, value in enumerate(row):
                col = columns[i]
                if col not in keep:
                    continue
                if isinstance(value, bytes):
                    value = base64.b64encode(value).decode("utf-8")
                if col.lower() == "msisdn":
                    value = clean_msisdn(value)
                row_dict[col] = value
            out.append(row_dict)

        logger.info(f"/tesdata -> {len(out)} rows")
        return jsonify(out)
    except Exception as e:
        logger.error(f"/tesdata error: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route("/SatuDatabase")
def sync_to_gsheet():
    # Selalu chunked (default 500). Bisa override via ?chunk_size=###
    db = request.args.get("db")
    if not db:
        return jsonify({"error": "Provide ?db=<database_name>"}), 400
    try:
        data = build_data_for_db(db)
        if not data:
            return jsonify({"message": "No data to push"})
        try:
            chunk_size = max(1, int(request.args.get("chunk_size", str(CHUNK_SIZE_DEFAULT))))
        except Exception:
            chunk_size = CHUNK_SIZE_DEFAULT
        summary = post_in_chunks(data, chunk_size=chunk_size, max_retries=2, retry_delay=1.0)
        return jsonify({"db": db, "chunk_size": chunk_size, **summary})
    except Exception as e:
        logger.error(f"/SatuDatabase error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/BanyakDatabase")
def sync_batch_to_gsheet():
    # Selalu chunked
    all_data, db_errors = [], []
    try:
        refresh_databases()
        try:
            chunk_size = max(1, int(request.args.get("chunk_size", str(CHUNK_SIZE_DEFAULT))))
        except Exception:
            chunk_size = CHUNK_SIZE_DEFAULT

        for db_name in databases:
            try:
                logger.info(f"Reading DB: {db_name}")
                rows = build_data_for_db(db_name)
                if rows:
                    all_data.extend(rows)
                    logger.info(f"  -> {len(rows)} rows")
            except Exception as e:
                msg = f"{db_name}: {e}"
                logger.error(msg)
                db_errors.append(msg)

        if not all_data:
            return jsonify({"message": "No data to push", "db_errors": db_errors})

        summary = post_in_chunks(all_data, chunk_size=chunk_size, max_retries=2, retry_delay=1.0)
        return jsonify({
            "databases_processed": len(databases),
            "chunk_size": chunk_size,
            "db_errors": db_errors,
            **summary
        })
    except Exception as e:
        logger.error(f"/BanyakDatabase error: {e}")
        return jsonify({"error": str(e), "db_errors": db_errors}), 500


# ================= Web UI (pakai index.html kamu) =================

@app.route("/")
def index():
    try:
        refresh_databases()
    except ValueError:
        pass
    return render_template(
        "index.html",
        gas_url=get_gas_url(),
        server_type=get_server_type(),
        databases=databases,
        message=None
    )


@app.route("/set_gas_url", methods=["POST"])
def set_gas_url():
    global GAS_URL
    val = request.form.get("gas_url", "").strip()
    if not val:
        flash("Please provide a non-empty URL")
        return redirect(url_for("index"))
    try:
        with open("url", "w", encoding="utf-8") as f:
            f.write(val)
        GAS_URL = val
        logger.info("GAS URL saved.")
        return redirect(url_for("index"))
    except Exception as e:
        logger.error(f"Save GAS URL failed: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/set_server_type", methods=["POST"])
def set_server_type_route():
    val = request.form.get("server_type", "").strip().upper()
    if val not in ["EXPRESS", "DEFAULT"]:
        flash("Please select a valid server type")
        return redirect(url_for("index"))
    try:
        save_server_type(val)
        try:
            refresh_databases()
        except Exception as e:
            flash(f"Server type saved but failed to connect: {e}")
        return redirect(url_for("index"))
    except Exception as e:
        logger.error(f"Save server type failed: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/refresh_databases", methods=["POST"])
def refresh_databases_route():
    try:
        refresh_databases()
        return redirect(url_for("index"))
    except Exception as e:
        logger.error(f"Refresh DBs error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/sync_one", methods=["POST"])
def sync_one():
    # UI: selalu chunked default 500
    db = request.form.get("database")
    if not db:
        return jsonify({"error": "No database selected"}), 400
    try:
        data = build_data_for_db(db)
        if not data:
            return render_template("index.html",
                                   gas_url=get_gas_url(),
                                   server_type=get_server_type(),
                                   databases=databases,
                                   message="No data to push")
        summary = post_in_chunks(data, chunk_size=CHUNK_SIZE_DEFAULT, max_retries=2, retry_delay=1.0)
        msg = (
            f"Pushed {summary['total_sent']} rows from {db} "
            f"in {summary['total_chunks']} chunks (size {CHUNK_SIZE_DEFAULT})."
        )
        return render_template("index.html",
                               gas_url=get_gas_url(),
                               server_type=get_server_type(),
                               databases=databases,
                               message=msg)
    except Exception as e:
        logger.error(f"sync_one error: {e}")
        return render_template("index.html",
                               gas_url=get_gas_url(),
                               server_type=get_server_type(),
                               databases=databases,
                               message=str(e)), 500


@app.route("/sync_batch", methods=["POST"])
def sync_batch():
    # UI: selalu chunked default 500
    try:
        refresh_databases()
        all_data, errors_collect = [], []

        for db_name in databases:
            try:
                rows = build_data_for_db(db_name)
                if rows:
                    all_data.extend(rows)
            except Exception as e:
                errors_collect.append(f"{db_name}: {e}")

        if not all_data:
            return render_template("index.html",
                                   gas_url=get_gas_url(),
                                   server_type=get_server_type(),
                                   databases=databases,
                                   message=f"No data to push. Errors: {errors_collect}")

        summary = post_in_chunks(all_data, chunk_size=CHUNK_SIZE_DEFAULT, max_retries=2, retry_delay=1.0)

        msg_parts = [
            "Batch sync finished.",
            f"Pushed {summary['total_sent']} rows from {len(databases)} DBs.",
            f"Chunks sent: {summary['total_chunks']} (size {CHUNK_SIZE_DEFAULT}).",
        ]
        if errors_collect:
            msg_parts.append(f"DB read errors: {errors_collect}")
        if summary["errors"]:
            msg_parts.append(f"Send errors: {summary['errors'][:5]}")

        preview = [f"chunk#{r['chunk']} rows={r['rows']} status={r['status']}" for r in summary["results"][:3]]
        if preview:
            msg_parts.append("Sample chunk status: " + " | ".join(preview))

        if summary["failed_chunks"]:
            failed_preview = ", ".join(f"chunk#{f['chunk']} rows={f['rows']}" for f in summary["failed_chunks"][:5])
            msg_parts.append(f"Failed chunks (need retry): {failed_preview}.")

        return render_template("index.html",
                               gas_url=get_gas_url(),
                               server_type=get_server_type(),
                               databases=databases,
                               message=" ".join(msg_parts))
    except Exception as e:
        logger.error(f"sync_batch error: {e}")
        return render_template("index.html",
                               gas_url=get_gas_url(),
                               server_type=get_server_type(),
                               databases=databases,
                               message=str(e)), 500


# ================= Optional: auto-open browser =================
def open_browser():
    webbrowser.open_new("http://127.0.0.1:5000/")


if __name__ == "__main__":
    get_gas_url()
    get_server_type()
    # import threading; threading.Timer(1.5, open_browser).start()
    app.run(debug=True, port=5000)
