# Use the lightweight Python 3.11 image
FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file first (for better caching)
COPY requirements.txt .

# Install dependencies
# We upgrade pip to ensure compatibility with 3.11 wheels
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY sync_brevo_supabase.py .

# Command to run the script
CMD ["python", "sync_brevo_supabase.py"]