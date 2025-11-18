FROM fishtownanalytics/dbt:1.10.15

# Set the working directory
WORKDIR /usr/app

# Copy the project files into the container
COPY . .

# Install any additional Python dependencies
RUN pip install -r requirements.txt

# Command to run dbt
CMD ["dbt", "run"]