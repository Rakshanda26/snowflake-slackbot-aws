# Step 1: Use official Python slim image
FROM python:3.8-slim

# Step 2: Set working directory inside container
WORKDIR /app

# Step 3: Copy your application code into the container
COPY app.py .

# Step 4: Install required Python packages
RUN pip install --no-cache-dir flask snowflake-connector-python requests

# Step 5: Expose Flask port (3000)
EXPOSE 3000

# Step 6: Start the Flask app
CMD ["python", "app.py"]
