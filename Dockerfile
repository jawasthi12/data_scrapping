# Use an official Python runtime as a parent image
FROM python:3.12-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install required Python packages directly
RUN pip install pandas pyspark requests psycopg2-binary

# Set the path to the Python executable (optional if Python is properly installed)
ENV PATH="/usr/local/bin:${PATH}"

# Run your script when the container launches
CMD ["python", "/app/GetCsvData.py"]
