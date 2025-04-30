# Use official Python image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install dependencies first (for better caching)
RUN pip install --no-cache-dir requests beautifulsoup4 python-dotenv schedule selenium

# Copy all files to the container
COPY . .

# Create an empty .env file if it doesn't exist
RUN touch .env

# Make the script executable
RUN chmod +x parsely.py

# Set the entrypoint
ENTRYPOINT ["python", "parsely.py"]
