# This is a properties file which specifies certain aspects of the deployment on kubernetes
# In jenkins these variables will be passed onto the CD job as parameters

# ENV_NAME specifies the "apigee" prefixed namespace where this application will be deployed
# In this example the namespace will be "apigee-jenkins"
ENV_NAME="jenkins"

# REPLICAS specifies the number of pod “replicas” running at any one time. In other words, the
# number inputted is the amount of pods that are always up and available.
REPLICAS=1
