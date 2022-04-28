# How to run

## Prereq

1. Install Python 3.x [https://www.python.org/downloads/](https://www.python.org/downloads/)
2. Install Python Virtualenv [https://virtualenv.pypa.io/en/latest/installation.html](https://virtualenv.pypa.io/en/latest/installation.html)

## Required packages

1. Create a virtual environment
```bash
virtualenv spark-env
```

2. Activate virtual environment
```bash
source spark-env/bin/activate
```

3. Install PySpark
```bash
pip install pyspark psutil nbconvert ipykernel
```

4. In Visual Studio Code install Jupyer tools [https://marketplace.visualstudio.com/items?itemName=ms-toolsai.jupyter](https://marketplace.visualstudio.com/items?itemName=ms-toolsai.jupyter)

5. When you open a Notebook - select python enterpreter from virtual environment