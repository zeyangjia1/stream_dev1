from pyspark import SparkConf, SparkContext
import os  # 导入os模块用于设置环境变量
def showResult(one):
    print(one,"结果")
if __name__ == "__main__":

    python_exec_path = "D:/anaconda/envs/py/python.exe"
    os.environ["PYSPARK_PYTHON"] = python_exec_path
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_exec_path
    conf = SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("WordCount")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    sc._jsc.hadoopConfiguration().set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
    lines = sc.textFile("D:/idea_work/Stream_dev/py/words")
    words = lines.flatMap(lambda line: line.split(" ")) \
        .filter(lambda word: word.strip() != "")  # 过滤分割产生的空字符串
    pairWords = words.map(lambda word: (word, 1))
    reduceResult = pairWords.reduceByKey(lambda v1, v2: v1 + v2)
    print("词频统计结果：")
    reduceResult.foreach(showResult)
    sc.stop()