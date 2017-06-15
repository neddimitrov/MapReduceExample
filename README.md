# MapReduceExample
Map Reduce using IPython.parallel

Execute the program using the following steps:

1. ipython has to be installed because the program depends on IPython.parallel

2. in the same directory as all the .py and .txt files, execute "ipcluster start --n=4".  This starts a cluster with a controller and 4 engines on the local machine.  It is also possible to start an engine on a different machine and have it be controlled by this controller.  For that, read the IPython.parallel documentation.

3. Wait until all the engines have started -- a little after the print statement "Starting 4 Engines..."

4. Execute "python example.py"  The print statements execute the word count example and the movies-ratings example.

