import great_expectations as ge
import json
import tkinter as tk
from tkinter import filedialog
from pyspark.sql import Row, SparkSession
root = tk.Tk()
root.withdraw()
print("Select expectations suite")
file_path = filedialog.askopenfilename()


with open(file_path,"r") as json_file:
    valid_suit= json.load(json_file)
print("Select data file")
m= filedialog.askopenfilename()

my_df = ge.read_csv(m, expectation_suite = valid_suit)


re= my_df.validate()


if not re.success:
    print("Data Quality test failed")
