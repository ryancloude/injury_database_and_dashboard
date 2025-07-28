# Use a slim Python base image to reduce container size
FROM python:3.11-slim

# Set working directory inside the container
WORKDIR /app

# Install OS-level dependencies and Python libraries
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./scripts ./scripts

#Keeps the container running for exec access

CMD ["tail", "-f", "/dev/null"]  