# Instructions for Deploying Streamlit with Google Run

## Step 1: Create Your Streamlit Application

To begin the deployment process on Google Cloud Run, start by creating your application using Streamlit. Make sure to thoroughly test it locally to identify and resolve any errors before proceeding with containerization.

## Step 2: Containerize the Application

1. Create de Docker file
    ```dockerfile
    FROM python:3.8
    EXPOSE 8080
    WORKDIR /app
    COPY . /app
    RUN pip install -r requirements.txt
    ENTRYPOINT ["streamlit", "run", "main.py", "--server.port=8080", "--server.address=0.0.0.0"]
    CMD ["paramd"]```

2. Create the requirements.txt file

   Create a requirements.txt file to specify all the necessary libraries for your Streamlit application. 
   ```
    pandas   
    matplotlib
    wordcloud
    streamlit
    google-cloud-bigquery
    matplotlib
    seaborn
    scikit-learn
    nltk
    db-dtypes
   ```

3. Build and Submit the Docker Image on Google Cloud Shell


   In the Google Cloud Shell, execute the following command to build and submit the Docker image to Google Container Registry (GCR). Replace `<PROJECT_ID>` with your project's name and `<SOME_PROJECT_NAME>` with the chosen name for your app:

   ```bash
   gcloud builds submit --tag gcr.io/<PROJECT_ID>/<SOME_PROJECT_NAME> --timeout=2h 
   ```
    Before executing the build command, confirm that all necessary files, including your Streamlit app, Dockerfile, and requirements.txt, are present in the Google Cloud Shell. Any missing files may result in errors during the build process due to an inability to locate the required files.

   Use the following commands to check and navigate to the correct directory:

   ```bash
   ls  # List files in the current directory
   cd <path_to_directory_containing_files>  # Change to the directory containing your files
    ```

## Step 3: Deploy on Google Cloud Run

In the Navigation Menu of your Google Cloud Console, locate and select "Google Cloud Run." Click on "Create Service" to initiate the creation of a new service. Google Cloud Run should automatically recognize the container you uploaded during the build process, allowing you to select it for deployment.

![imagen](..\assets\Cloudrun.png)

Make sure to check the box that says “Allow unauthenticated invocations” when setting up your service. This allows the public to access your url.

## Step 4: Verify Deployment

Once the deployment process is complete, Google Cloud Run will provide you with a public URL where your Streamlit Web App is accessible. This URL can be used to view and share your application.

Open a web browser and navigate to the provided URL to explore and interact with your Streamlit Web App. Share this URL with others to showcase your deployed application.


![Alt text](..\assets\Cloudrunurl.png)

