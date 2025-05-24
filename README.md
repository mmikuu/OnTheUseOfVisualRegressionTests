# OnTheUseOfVisualRegressionTests
This repository includes the replication package and results for ESEM2025 (Short Paper). 
If you want to use this tool in your research, please cite the following papers:

## Source code
Our main source code is located in 'vrt_comment' directory.

### Run 
Please execute the following description

Analytics data
1. You can run it with the following commands:
```
python3 calculate-data.py
```

Collect data
1. You need to obtain GitHub tokens
2. Write the obtained tokes in "GITHUB_TOKEN"
3. You can run all "main*.py" files in "vrt_comment":

### Outputted file 
The program will generate "data/Classification.csv" that caontains the list of the links for discussion including VRT link.

## Annotated results
With the output of the above program, two of the authors performed the manual inspection independently and manually. 
The annotated classification result is stored in the "results/annotations/Classification.csv" file. 