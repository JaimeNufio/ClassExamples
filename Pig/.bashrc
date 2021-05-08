the changes:

export SPARK_HOME='/spark-3.1.1-bin-hadoop2.7'
export PATH=$SPARK_HOME:$PATH
export JAVA_HOME='/usr/lib/jvm/java-8-openjdk-amd64'
export PATH=$JAVA_HOME/bin:$PATH
export PYTHONPATH=$SPARK_HOME/python:$PATH
export PYSPARK_DRIVER_PYTHON="jupyter"
export PYSPARK_DRIVER_PYTHON_OPTS="notebook"
export PYSPARK_PYTHON=python3
export PIG_HOME='/home/nufio/Downloads/pig-0.16.0'
export PATH=$PIG_HOME/bin:$PATH
