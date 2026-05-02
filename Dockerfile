FROM apache/airflow:2.7.1-python3.10

# Copy your requirements file into the image
COPY requirements.txt /requirements.txt

# Install the libraries once during the build process
RUN pip install --no-cache-dir -r /requirements.txt