from flask import Flask, render_template, request, redirect, url_for, session, send_from_directory
import pyodbc
import csv
import os
import time
import html
import xml.etree.ElementTree as ET

app = Flask(__name__)
app.secret_key = "secure_key_for_session"

def extract_missing_index_suggestion(plan_xml):
    try:
        root = ET.fromstring(plan_xml)
        namespaces = {'sql': 'http://schemas.microsoft.com/sqlserver/2004/07/showplan'}
        suggestions = []

        for missing_index_group in root.findall(".//sql:MissingIndexes/sql:MissingIndexGroup", namespaces):
            for missing_index in missing_index_group.findall("sql:MissingIndex", namespaces):
                table = missing_index.attrib.get("Table", "")
                schema = missing_index.attrib.get("Schema", "dbo")

                equality_columns = [col.attrib['Name'] for col in missing_index.findall("sql:Column[@Usage='EQUALITY']", namespaces)]
                inequality_columns = [col.attrib['Name'] for col in missing_index.findall("sql:Column[@Usage='INEQUALITY']", namespaces)]
                include_columns = [col.attrib['Name'] for col in missing_index.findall("sql:Column[@Usage='INCLUDE']", namespaces)]

                index_sql = f"CREATE NONCLUSTERED INDEX [IX_AutoGen] ON [{schema}].[{table}] ("
                index_sql += ", ".join(equality_columns + inequality_columns) + ")"
                if include_columns:
                    index_sql += " INCLUDE (" + ", ".join(include_columns) + ")"
                suggestions.append(index_sql)

        return suggestions
    except Exception as e:
        return [f"Failed to parse execution plan XML: {str(e)}"]

@app.route("/", methods=["GET"])
def welcome():
    return render_template("welcome.html")

@app.route("/connect", methods=["POST"])
def connect():
    connection_string = request.form.get("connection_string")
    try:
        conn = pyodbc.connect(connection_string)
        conn.close()
        session["connection_string"] = connection_string
        return redirect(url_for("index"))
    except Exception as e:
        return render_template("welcome.html", message=f"Connection Error: {str(e)}")

@app.route("/optimizer", methods=["GET", "POST"])
def index():
    query_result = None
    message = ""
    exec_time = None
    execution_summary = None
    connection_string = session.get("connection_string")
    report_filename = session.get("report_filename")

    if not connection_string:
        return redirect(url_for("welcome"))

    if request.method == "POST":
        sql_query = request.form["query"]
        report_filename = None

        try:
            conn = pyodbc.connect(connection_string)
            cursor = conn.cursor()

            # Get actual execution plan and execution summary
            cursor.execute("SET STATISTICS XML ON")
            start_time = time.time()
            cursor.execute(sql_query)
            rows = cursor.fetchall()
            exec_time = time.time() - start_time
            columns = [column[0] for column in cursor.description]
            query_result = [dict(zip(columns, row)) for row in rows]

            # Fetch execution plan XML
            cursor.nextset()
            plan_xml = ""
            for row in cursor:
                plan_xml = row[0]
            cursor.execute("SET STATISTICS XML OFF")

            # Extract execution summary and missing index suggestion
            execution_summary = [
                f"Query executed with relative cost 100%",
                f"Executed Query: {sql_query.strip()}"
            ]

            if plan_xml:
                index_suggestions = extract_missing_index_suggestion(plan_xml)
                if index_suggestions:
                    execution_summary.append("Missing Index Suggestion:")
                    execution_summary.extend(index_suggestions)

            # Save to CSV
            base_filename = "query_performance_report"
            counter = 1
            filename = f"{base_filename}_{counter}.csv"
            while os.path.exists(filename):
                counter += 1
                filename = f"{base_filename}_{counter}.csv"
            with open(filename, "w", newline="") as file:
                writer = csv.writer(file)
                writer.writerow(columns)
                writer.writerows(rows)

            message = f"✅ Query executed successfully. {len(rows)} rows returned."
            report_filename = filename
            session["report_filename"] = report_filename

        except Exception as e:
            message = f"❌ Error: {str(e)}"
            query_result = []
            execution_summary = []
            session.pop("report_filename", None)

    return render_template(
        "index.html",
        query_result=query_result,
        message=message,
        execution_time=f"{exec_time:.2f}" if exec_time else None,
        execution_summary=execution_summary,
        report_filename=report_filename
    )

@app.route("/download_report")
def download_report():
    filename = session.get("report_filename")
    if not filename or not os.path.exists(filename):
        return redirect(url_for("index"))

    directory = os.path.abspath(os.path.dirname(filename))
    return send_from_directory(directory, os.path.basename(filename), as_attachment=True)

if __name__ == "__main__":
    app.run(debug=True)
