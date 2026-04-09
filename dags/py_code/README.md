### Step 1: Create virtual environment as normal
virtualenv myenv

### Step 2: Install Jupyter Notebook into virtual environment
python3 -m pip install ipykernel

### Step 3: Install a python package
python3 -m pip install selenium

### Step 3.1: Allow Jupyter access to the kernel within our new virtual environment
python3 -m ipykernel install --user --name=myenv

