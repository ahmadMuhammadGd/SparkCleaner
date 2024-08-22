import sys, os
import glob


tests_dir='./tests'
python_pattern='*.py'
python_path_pattern = os.path.join(tests_dir, python_pattern)

for path in glob.glob(python_path_pattern):
    exec(open(path).read())