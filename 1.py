isNeedInstall = False
if isNeedInstall:
    import subprocess
    import sys

    def install(package):
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])

    install("pyspark")
    install("psutil")
    install("nbconvert")
    install("ipykernel")
    install("py4j")

from pyspark import SparkConf, SparkContext
import os
import shutil

isProd = True

number_cores = 2
memory_gb = 4

conf = (
    SparkConf()
        .setAppName("mandarin")
        .setMaster('local[{}]'.format(number_cores))
        .set('spark.driver.memory', '{}g'.format(memory_gb))
)

sc = SparkContext.getOrCreate()

if isProd:
    if not os.path.exists('input/Reviews.csv'):
        sc.stop()
        raise Exception("""
            Download the 'Reviews.csv' file from https://www.kaggle.com/datasets/snap/amazon-fine-food-reviews
            and put it in 'input' folder
        """)
    else:
        inputRdd = sc.textFile("input/Reviews.csv")
else:
    inputRdd = sc.textFile("input/Sample.csv")
    
filteredInput = inputRdd.filter(lambda line: line.startswith("Id,") == False)

userProductMap = filteredInput.map(lambda x: x.split(",")[2] + "," + x.split(",")[1]).map(lambda x: x.split(","))
userProductMap.collect()    

zero_value = set()

def seq_op(x,y):
    x.add(y)
    return x

def comb_op(x,y):
    return x.union(y)

userProducts = userProductMap.aggregateByKey(zero_value, seq_op, comb_op).sortByKey()
userProducts.collect()

productPairsMap = list(userProducts.reduceByKey(lambda a,b: b.lookup(a)).map(lambda r: r[1]).filter(lambda x: len(x)>1).collect())
print(productPairsMap)

i = 0
j = 0 
tupleProduct = []
tempList = []
for x in productPairsMap:
    tempList.append(list(x))
while i < len(tempList):
    while j<len(tempList[i])-1:
        tupleProduct.append(tempList[i][j]+tempList[i][j+1])
        j+=1
    i+=1
    j=0
print(tupleProduct)

nextStep = sc.parallelize(tupleProduct)
tupleProductWithOne = nextStep.map(lambda x: (x,1))
tupleProductWithOne.collect()

allTupleProd = list(tupleProductWithOne.countByKey().items())
tempProd = sc.parallelize(allTupleProd)
productPairsCounts = tempProd.sortBy(lambda x: -x[1])
productPairsCounts.collect()

result = productPairsCounts.zipWithIndex().filter(lambda vi: vi[1] < 10).keys()
result.collect()

outpath = 'output/first_output'
if os.path.exists(outpath) and os.path.isdir(outpath):
    shutil.rmtree(outpath)

result.saveAsTextFile(outpath)

sc.stop()