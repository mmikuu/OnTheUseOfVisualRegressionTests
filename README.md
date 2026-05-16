# OnTheUseOfVisualRegressionTests
This repository includes the replication package and results for ICSME2026 (Short Paper). 
If you want to use this tool in your research, please cite our paper:

## Source code
Our main source code is located in 'vrt_comment' directory.

### Run 
Please follow the instructions below

Collect data
1. You need to obtain GitHub tokens
2. Write the obtained tokens in "GITHUB_TOKEN"
3. You can run all "main*.py" files in "vrt_comment/module":

Data Analysis
1. You can run it with the following commands:
```
python3 ./vrt_comment/analyze/calculate-data.py
```

### Output file 
The program will generate "results/analytics/result.csv" which calculates effectiveness measurements based on VRT comments.

## Annotated results
With the output of the above program, two of the authors performed the manual inspection independently and manually. 
The annotated classification result is stored in the "results/annotations/Classification.csv" file. 
