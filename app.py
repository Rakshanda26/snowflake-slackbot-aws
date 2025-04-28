import time
import threading
import re
import requests
import snowflake.connector
from flask import Flask, request, jsonify

app = Flask(__name__)

# Snowflake Connection
sf_connection = snowflake.connector.connect(
    user="",
    password="",
    account="",
    warehouse='',
    database='',
    schema='public',
    session_parameters={'QUERY_TIMEOUT': 3600}
)

def grant_permissions():
    cursor = sf_connection.cursor()
    try:
        cursor.execute("GRANT USAGE ON WAREHOUSE my_wh TO ROLE SALES_ROLE;")
        cursor.execute("GRANT USAGE ON DATABASE my_cortex_demo TO ROLE SALES_ROLE;")
        cursor.execute("GRANT USAGE ON SCHEMA public TO ROLE SALES_ROLE;")
        cursor.execute("GRANT SELECT ON TABLE public.SALES TO ROLE SALES_ROLE;")
        print("Permissions granted successfully.")
    finally:
        cursor.close()

grant_permissions()

# ========== Main Query Processing ==========
def process_snowflake_query(user_question, slack_response_url):
    try:
        cursor = sf_connection.cursor()

        # Step 1: Generate SQL via Cortex with strict prompt
        prompt = f"""
        Context URL: https://snowflake-cortex-schemas.s3.us-east-1.amazonaws.com/schema.yaml
        Instruction:
        The SALES table has columns REGION, SALES_AMOUNT, and SALE_DATE.
        Write a Snowflake SQL query using only these columns to answer the following question:
        {user_question}
        """

        cortex_query = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            'llama3-8b',
            $$ {prompt} $$
        ) AS generated_sql;
        """

        cursor.execute(cortex_query)
        result = cursor.fetchone()
        full_response = result[0]
        
        print(f"Full response from Cortex: {full_response}")

        # Step 2: Extract SQL using regex
        match = re.search(r"(SELECT[\s\S]+?;)", full_response, re.IGNORECASE)
        if not match:
            raise Exception("No valid SQL found in Cortex response.")

        generated_sql = match.group(1)
        print(f"Generated SQL: {generated_sql}")

        # Step 3: Execute the generated SQL
        cursor.execute(generated_sql)
        sales_result = cursor.fetchone()

        if sales_result and sales_result[0] is not None:
            total_sales = sales_result[0]
        else:
            total_sales = 0

        # Step 4: Summarize the result
        summary_prompt = f"The total sales based on your question '{user_question}' is ${total_sales}."
        summarize_query = f"""
        SELECT SNOWFLAKE.CORTEX.SUMMARIZE(
            $$ {summary_prompt} $$
        ) AS summary;
        """

        cursor.execute(summarize_query)
        summary_result = cursor.fetchone()
        summary = summary_result[0] if summary_result else summary_prompt

        # Step 5: Send the summarized response back to Slack
        response = {
            "response_type": "in_channel",
            "text": f"*Question:* {user_question}\n*Answer:* {summary}"
        }
        requests.post(slack_response_url, json=response)

    except Exception as e:
        print(f"Error: {str(e)}")
        response = {
            "response_type": "ephemeral",
            "text": f"Error: {str(e)}"
        }
        requests.post(slack_response_url, json=response)

    finally:
        cursor.close()

# ========== Flask Route ==========
@app.route("/slack/command", methods=["POST"])
def slack_command():
    data = request.form
    user_question = data.get("text")
    slack_response_url = data.get("response_url")

    # Immediate acknowledgement to Slack
    ack_response = {
        "response_type": "ephemeral",
        "text": "Request has been queued. Please wait for the result..."
    }
    
    # Start background thread for heavy processing
    thread = threading.Thread(target=background_process, args=(user_question, slack_response_url))
    thread.start()

    return jsonify(ack_response)

# ========== Background Worker ==========
def background_process(user_question, slack_response_url):
    time.sleep(5)  # ⏰ Wait 5 seconds to simulate processing delay
    process_snowflake_query(user_question, slack_response_url)

# ========== Flask Home Page ==========
@app.route("/", methods=["GET"])
def home():
    return "Snowflake Slackbot is running."

# ========== Start Server ==========
if __name__ == "__main__":
    app.run(port=3000, debug=True)
