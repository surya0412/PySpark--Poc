import shutil
shutil.make_archive("dist/src", 'zip', "src")
shutil.copyfile("main.py","dist/main.py")
shutil.copyfile("log4j.properties","dist/log4j.properties")
# shutil.copyfile("spark.conf","dist/spark.conf")