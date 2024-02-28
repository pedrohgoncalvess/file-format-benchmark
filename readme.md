# File formats benchmark

This script was initially developed to assist in the argumentation of an article. Feel free to use the script and read the article for more context.

## Running project

To run this project I recommend that you create a Python virtual environment that you can easily run the following command in the terminal.
```
python -m venv /dir_name
```
*You can change dir_name for any other dir name.*

Then activate the venv com by running the following command in the terminal.
```
dir_name\Scripts\activate
```

Install the required libraries.
```
pip install -r requirements.txt
```

**For this project you will need spark and hadoop set in the environment variables, I recommend following the PySpark installation manual.**

### [PySpark installation manual](https://spark.apache.org/docs/latest/api/python/getting_started/install.html)

![img.png](images/img.png)

## Benchmark.conf

The benchmark.conf file asks for some settings for the project to run. The first is the query that will be used to benchmark the query between the files, the second is the number of benchmarks that should be done, and the third is the benchmarks that you want to be done