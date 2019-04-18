# Make a copy of this file and enter the appropriate values below. Name the file as defines.py
hostname=""   # Please enter the host name of Django webserver
hostport=""  # Enter the port of Django server
LIBTECH_AWS_ACCESS_KEY_ID = ""  # AWS Access Key
LIBTECH_AWS_SECRET_ACCESS_KEY = ""   # AWS Secret Key
LIBTECH_AWS_BUCKET_NAME=""  # Bucket Name
AWS_S3_REGION_NAME = ''  # AWS Region na;me
djangoDir=""  # Full path of source directory
repoDir=""  # Not required any more. 
djangoSettings="libtech.settings"
logDir=""  # Log directory
crawlerLogDir=""  # Crawler Log Directory
apiusername=''  # Django API User name
apipassword=''  # Django rest api user password
AUTHENDPOINT='http://%s:%s/auth/jwt/' % (hostname,hostport)
BASEURL="http://%s:%s/api/" % (hostname,hostport)
