from flask import Flask, jsonify
import snowflake.connector
import re  # For regex to extract SQL

app = Flask(__name__)

# Snowflake connection setup
sf_connection = snowflake.connector.connect(
    user="RADHIKA",
    password="Radhamadhav@55",
    account="JLZHQNI-HWB51415",
    warehouse='my_wh',
    database='my_cortex_demo',
    schema='public'
)

@app.route('/ask', methods=['GET'])
def ask_snowflake():
    try:
        cursor = sf_connection.cursor()

        # 1️⃣ Hardcoded question
        user_question = "What were total sales in Q1 2024?"

        # 2️⃣ Prepare instruction for Cortex
        prompt = """
        Context URL: https://snowflake-cortex-schemas.s3.us-east-1.amazonaws.com/schema.yaml
        Instruction: Write a SQL query to calculate total SALES_AMOUNT from the SALES table 
        where SALE_DATE is between 2024-01-01 and 2024-03-31.
        """

        # 3️⃣ Generate SQL using Cortex
        generate_sql_query = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            'llama3-8b',
            $$ {prompt} $$
        ) AS generated_sql;
        """

        cursor.execute(generate_sql_query)
        result = cursor.fetchone()
        full_response = result[0]  # This has both explanation + SQL

        # 4️⃣ Use regex to extract only the SQL (SELECT ... till semicolon)
        match = re.search(r"(SELECT[\s\S]+?;)", full_response, re.IGNORECASE)
        if not match:
            raise Exception("No valid SQL found in Cortex response.")
        
        generated_sql = match.group(1)

        # 5️⃣ Execute the extracted SQL
        cursor.execute(generated_sql)
        sales_result = cursor.fetchone()
        total_sales = sales_result[0] if sales_result else 0

        # 6️⃣ Summarize using Cortex
        summary_prompt = f"Total sales in Q1 2024 were ${total_sales}."
        summarize_query = f"""
        SELECT SNOWFLAKE.CORTEX.SUMMARIZE($$ {summary_prompt} $$) AS summary;
        """

        cursor.execute(summarize_query)
        summary_result = cursor.fetchone()
        summary = summary_result[0] if summary_result else summary_prompt

        return jsonify({
            "question": user_question,
            "summary": summary
        })

    except Exception as e:
        return jsonify({"error": str(e)})

@app.route('/')
def home():
    return "Flask app running. Hit /ask to test Cortex SQL generation and summary."

if __name__ == '__main__':
    app.run(port=3000, debug=True)
